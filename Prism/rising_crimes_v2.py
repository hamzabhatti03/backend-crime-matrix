"""
rising_crimes_v1.py

cron job that will execute after every 2 hour to detect rising crime alerts (Level 3 categories under "Crime Against Property").
Filters the last 6 days of data, computes two 3-day windows of rolling averages,
identifies excessive cases and writes to rising_crimes table.
"""


import sys
import os
import logging
from datetime import date, timedelta
from typing import List, Tuple, Dict, Any
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Utilities.utils import get_processed_db_connection
import pandas as pd
import psycopg2
import warnings

# --- Configuration Constants ---
DAYS_WINDOW = 10
DAYS_LOOKBACK = DAYS_WINDOW * 2     # 20 days total
FUZZY_THRESHOLD = 80

# Configure logging
log_dir = os.path.join(os.path.dirname(__file__), '..', 'Logs')
os.makedirs(log_dir, exist_ok=True)

log_path = os.path.join(log_dir, 'rising_crime.log')

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Log Execution start
logging.info("Rising Crime ETL Started")

# --- Data Extraction ---
def fetch_response_times(conn: psycopg2.extensions.connection, end_date: date) -> pd.DataFrame:
    """
    Fetch all 'Crime Against Property' calls for the 6-day window ending at end_date.
    """
    start_date = (end_date - timedelta(days=DAYS_LOOKBACK)).strftime('%Y-%m-%d')
    sql = """
        SELECT *
        FROM response_time
        WHERE date BETWEEN %s AND %s
          AND level1_case_nature = 'Crime Against Property'
          AND level2_case_nature IN ('Dacoity','Burglary','Robbery/Snatching','Vehicle Theft','Vehicle Snatching') 
          AND police_station_id != 0 
          AND police_station is NOT NULL
          AND parent_id = 0
    """
    df = pd.read_sql(sql, conn, params=(start_date, end_date.strftime('%Y-%m-%d')))
    logging.info("Fetched %d response_time rows", len(df))
    return df


# --- Aggregation & Rolling Calculations ---
def compute_window_sums(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute sums and averages for the current and previous windows.
    Returns DataFrame with columns: police_station_id, level3_case_nature, current_sum, curr_avg, previous_sum, prev_avg
    """
    if df.empty:
        return pd.DataFrame(columns=["police_station_id", "level3_case_nature", "current_sum", "curr_avg", "previous_sum", "prev_avg"])

    # Group by police_station_id, level3_case_nature, and date to get daily counts
    daily_counts = df.groupby(["police_station_id", "level3_case_nature", "date"]).size().reset_index(name="count")

    # Pivot to get counts per day as columns
    pivot = daily_counts.pivot_table(
        index=["police_station_id", "level3_case_nature"],
        columns="date",
        values="count",
        fill_value=0
    )

    # Sort dates to ensure correct window selection
    all_dates = sorted(pivot.columns)

    # Check if we have enough data
    if len(all_dates) < 2 * DAYS_WINDOW:
        raise ValueError("Not enough days in data to compute both windows. Need at least 2 * DAYS_WINDOW days.")

    # Define current and previous window dates
    current_dates = all_dates[-DAYS_WINDOW:]
    previous_dates = all_dates[-2 * DAYS_WINDOW:-DAYS_WINDOW]

    # Compute sums
    current_sum = pivot[current_dates].sum(axis=1)
    previous_sum = pivot[previous_dates].sum(axis=1)

    # Create output DataFrame
    df_out = pd.DataFrame({
        "police_station_id": pivot.index.get_level_values(0),
        "level3_case_nature": pivot.index.get_level_values(1),
        "current_sum": current_sum,
        "previous_sum": previous_sum,
        "curr_avg": current_sum / DAYS_WINDOW,
        "prev_avg": previous_sum / DAYS_WINDOW
    })

    return df_out


def identify_rises(agg: pd.DataFrame) -> list[tuple[int, str, int]]:
    """
    Identify groups where curr_avg > prev_avg * 1.15 and compute excess cases.
    Returns list of (police_station_id, level3_case_nature, diff).
    """
    rises = []
    for _, row in agg.iterrows():
        if row["curr_avg"] > row["prev_avg"] * 1.15:  # More than 15% increase
            diff = int(row["current_sum"] - row["previous_sum"])  # Excess incidents
            if diff > 5:  # Ensure there’s an increase
                rises.append((row["police_station_id"], row["level3_case_nature"], diff))
    print(f"Found {len(rises)} groups with significant rises")
    return rises


# --- Case Selection & Matching ---
def fetch_excess_cases(
    conn: psycopg2.extensions.connection,
    ps: int,
    category: str,
    end_date: date,
    diff: int = None,
) -> pd.DataFrame:
    """
    Fetch all (or `diff` oldest) calls in the last DAYS_WINDOW days
    for the given PS + level3_case_nature.
    If `diff` is None, returns every row in the window.
    """
    start_date = (end_date - timedelta(days=DAYS_WINDOW)).strftime('%Y-%m-%d')
    end_str = end_date.strftime('%Y-%m-%d')

    sql = """
        SELECT *
        FROM response_time
        WHERE police_station_id = %s
          AND level3_case_nature = %s
          AND date BETWEEN %s AND %s
        ORDER BY created_time ASC
    """

    params = [ps, category, start_date, end_str]
    if diff is not None:
        sql += "\nLIMIT %s"
        params.append(diff)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        df = pd.read_sql(sql, conn, params=params)
    print(f"Fetched {len(df)} cases for PS={ps}, cat={category}")
    return df


def insert_rising_crimes(
    conn: psycopg2.extensions.connection,
    records: List[Dict[str, Any]],
):
    """
    Bulk-insert into rising_crimes. Assumes table exists with matching columns.
    """
    if not records:
        return

    df = pd.DataFrame(records)
    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    cols_sql      = ", ".join(cols)
    insert_sql = f"""
        INSERT INTO rising_crimes ({cols_sql})
        VALUES ({placeholders})
        ON CONFLICT (lead_id) DO NOTHING
    """

    with conn.cursor() as cur:
        data = [tuple(rec[col] for col in cols) for rec in records]
        cur.executemany(insert_sql, data)
    conn.commit()
    logging.info("Inserted %d records into rising_crimes", len(records))


def _col_def_sql(col_name: str, dtype: str, char_len: Any) -> str:
    """
    Helper to map all source columns to TEXT in the rising_crimes table.
    """
    # Regardless of the source data_type, store everything as TEXT
    return f"{col_name} TEXT"


def create_rising_crimes_table(conn: psycopg2.extensions.connection):
    """
    Inspect response_time and crime_hotspot schemas and build an empty
    rising_crimes table in the PRIMARY DB with rt_* and ch_* prefixed columns.
    """
    with conn.cursor() as cur:
        # Grab response_time columns from primary DB
        cur.execute("""
            SELECT column_name, data_type, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'response_time'
        """)
        rt_cols = cur.fetchall()

        # Build combined column definitions
        col_defs = []
        for col_name, dtype, char_len in rt_cols:
            col_defs.append(_col_def_sql(f"{col_name}", dtype, char_len))

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS rising_crimes (
                {', '.join(col_defs)},
                UNIQUE (lead_id)
            );
        """
        logging.info("Creating rising_crimes table if not exists...")
        cur.execute(create_sql)
        conn.commit()
        logging.info("rising_crimes table ready.")


def main():
    try:
        conn = get_processed_db_connection()
        create_rising_crimes_table(conn)

        # Now run *at runtime* rather than end-of-day:
        end_date = date.today()
        rt_df = fetch_response_times(conn, end_date)

        agg   = compute_window_sums(rt_df)
        rises = identify_rises(agg)

        all_matches = []
        for ps_id, cat, diff  in rises:
            try:
                # fetch *all* cases in that last 3-day window
                cases = fetch_excess_cases(conn, ps_id, cat, end_date, diff=None)
                all_matches.extend(cases.to_dict(orient="records"))
            except Exception as e:
                logging.error("Error on PS=%s, cat=%s: %s", ps_id, cat, e)

        insert_rising_crimes(conn, all_matches)

    except Exception as e:
        logging.critical("Script failed: %s", e, exc_info=True)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
