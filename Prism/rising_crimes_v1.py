"""
rising_crimes_v1.py

Daily cron job to detect rising crime alerts (Level 3 categories under "Crime Against Property").
Filters the last 6 days of data, computes two 3-day windows of rolling averages,
identifies excessive calls, fuzzy-matches locations against hotspots, and writes to rising_crimes.
"""

import logging
from datetime import date, timedelta
from typing import List, Tuple, Dict, Any
from Utilities.utils import get_processed_db_connection
import pandas as pd
import psycopg2
from rapidfuzz import fuzz

# --- Configuration Constants ---
DAYS_WINDOW = 3
DAYS_LOOKBACK = DAYS_WINDOW * 2     # 6 days total
FUZZY_THRESHOLD = 80

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s"
)
logger = logging.getLogger(__name__)

# --- Data Extraction ---
def fetch_response_times(conn: psycopg2.extensions.connection, end_date: date) -> pd.DataFrame:
    """
    Fetch all 'Crime Against Property' calls for the 6-day window ending at end_date.
    """
    start_date = (end_date - timedelta(days=DAYS_LOOKBACK - 1)).strftime('%Y-%m-%d')
    sql = """
        SELECT *
        FROM response_time
        WHERE date BETWEEN %s AND %s
          AND level1_case_nature = 'Crime Against Property' AND police_station is Not Null
    """
    df = pd.read_sql(sql, conn, params=(start_date, end_date.strftime('%Y-%m-%d')))
    logger.info("Fetched %d response_time rows", len(df))
    return df


# --- Aggregation & Rolling Calculations ---
def compute_window_sums(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns a DataFrame indexed by (police_station_id, level3_case_nature)
    with columns:
      prev_sum   = sum of days 1..3
      curr_sum   = sum of days 4..6
    """
    if df.empty:
        return pd.DataFrame(columns=["prev_sum", "curr_sum"])

    # Count per day
    grp = (
        df
        .groupby(["police_station", "level3_case_nature", "date"])
        .size()
        .reset_index(name="count")
    )

    # Ensure full date index per group
    all_idx = (
        grp
        .set_index(["police_station", "level3_case_nature", "date"])
        .unstack(fill_value=0)
        .fillna(0)
    )
    # Flatten back
    counts = (
        all_idx
        .stack(level=-1, future_stack=True)
        .reset_index()
        .rename(columns={0: "count"})
    )

    # Pivot to time series
    pivot = counts.pivot_table(
        index=["police_station", "level3_case_nature"],
        columns="date",
        values="count",
        fill_value=0
    )[sorted(counts["date"].unique())]

    rolled = (
        pivot
        .T  # dates become the index
        .rolling(window=DAYS_WINDOW)  # default axis=0 now works over dates
        .sum()
        .T  # transpose back
    )

    # Extract the two windows
    prev_cols = rolled.columns[:DAYS_WINDOW]
    curr_cols = rolled.columns[DAYS_WINDOW:DAYS_LOOKBACK]

    return pd.DataFrame({
        "police_station": rolled.index.get_level_values(0),
        "level3_case_nature": rolled.index.get_level_values(1),
        "prev_sum": rolled[prev_cols].sum(axis=1),
        "curr_sum": rolled[curr_cols].sum(axis=1),
    })


def identify_rises(agg: pd.DataFrame) -> List[Tuple[int, str, int]]:
    """
    Identify (ps_id, category, diff) where curr_sum > prev_sum.
    """
    rises = []
    for _, row in agg.iterrows():
        diff = int(row.curr_sum - row.prev_sum)
        if diff > 0:
            rises.append((
                row.police_station,
                row.level3_case_nature,
                diff
            ))
    logger.info("Identified %d rising PS+categories", len(rises))
    return rises


# --- Case Selection & Matching ---
def fetch_excess_cases(
    conn: psycopg2.extensions.connection,
    ps: int,
    category: str,
    end_date: date,
    diff: int,
) -> pd.DataFrame:
    """
    Fetch the 'diff' oldest calls in the current window for the given PS+category.
    """
    start_date = (end_date - timedelta(days=DAYS_WINDOW - 1)).strftime('%Y-%m-%d')
    sql = """
        SELECT *
        FROM response_time
        WHERE police_station = %s
          AND level3_case_nature = %s
          AND date BETWEEN %s AND %s
        ORDER BY created_time ASC
        LIMIT %s
    """
    df = pd.read_sql(sql, conn, params=(ps, category, start_date, end_date.strftime('%Y-%m-%d'), diff))
    logger.info("Fetched %d excess cases for PS=%s, cat=%s", len(df), ps, category)
    return df


def load_hotspots(
    conn: psycopg2.extensions.connection,
    ps: int,
    category: str,
) -> pd.DataFrame:
    """
    Load all hotspots for the given PS+category.
    """
    sql = """
        SELECT *
        FROM crime_hotspot
        WHERE police_station = %s
          AND case_nature = %s
    """
    df = pd.read_sql(sql, conn, params=(ps, category))
    logger.info("Loaded %d hotspots for PS=%s, cat=%s", len(df), ps, category)
    return df


def match_against_hotspots(
    cases: pd.DataFrame,
    hotspots: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    For each case, find the first hotspot with fuzzy location match ≥ threshold.
    Returns list of combined dicts with rt_ and ch_ prefixes.
    """
    matches = []
    for _, case in cases.iterrows():
        for _, hs in hotspots.iterrows():
            score = fuzz.token_set_ratio(case["caller_location"], hs["location"])
            if score >= FUZZY_THRESHOLD:
                # merge prefixed
                entry = {
                    **{f"rt_{col}": case[col] for col in case.index},
                    **{f"ch_{col}": hs[col] for col in hs.index},
                }
                matches.append(entry)
                break
    logger.info("Matched %d cases to hotspots", len(matches))
    return matches


# --- Persistence ---
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
        INSERT INTO prism_rising_crimes ({cols_sql})
        VALUES ({placeholders})
        ON CONFLICT (rt_lead_id) DO NOTHING
    """

    with conn.cursor() as cur:
        data = [tuple(rec[col] for col in cols) for rec in records]
        cur.executemany(insert_sql, data)
    conn.commit()
    logger.info("Inserted %d records into rising_crimes", len(records))


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

        # Grab crime_hotspot columns from PREDICTIVE DB
        pred_conn = get_processed_db_connection(database={
                                                        'dbname': 'db_predictive_policing',
                                                        'user': 'postgres',
                                                        'password': 'psca@officialmai1',
                                                        'host': '10.20.170.151',
                                                        'port': 5432
                                                        })
        with pred_conn.cursor() as pred_cur:
            pred_cur.execute("""
                SELECT column_name, data_type, character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'crime_hotspot'
            """)
            ch_cols = pred_cur.fetchall()
        pred_conn.close()

        # Build combined column definitions
        col_defs = []
        for col_name, dtype, char_len in rt_cols:
            col_defs.append(_col_def_sql(f"rt_{col_name}", dtype, char_len))
        for col_name, dtype, char_len in ch_cols:
            col_defs.append(_col_def_sql(f"ch_{col_name}", dtype, char_len))

        create_sql = f"""
            CREATE TABLE IF NOT EXISTS prism_rising_crimes (
                {', '.join(col_defs)},
                UNIQUE (rt_lead_id)
            );
        """
        logger.info("Creating rising_crimes table if not exists...")
        cur.execute(create_sql)
        conn.commit()
        logger.info("rising_crimes table ready.")


# --- Orchestration ---
def main():
    try:
        conn = get_processed_db_connection()
        create_rising_crimes_table(conn)
        pred_conn = get_processed_db_connection(database={
                                                        'dbname': 'db_predictive_policing',
                                                        'user': 'postgres',
                                                        'password': 'psca@officialmai1',
                                                        'host': '10.20.170.151',
                                                        'port': 5432
                                                        })
        # Use yesterday as last complete day
        end_date = date.today() - timedelta(days=1)

        rt_df = fetch_response_times(conn, end_date)
        agg = compute_window_sums(rt_df)
        rises = identify_rises(agg)

        all_matches = []
        for ps_id, cat, diff in rises:
            try:
                excess = fetch_excess_cases(conn, ps_id, cat, end_date, diff)
                hotspots = load_hotspots(pred_conn, ps_id, cat)
                matches = match_against_hotspots(excess, hotspots)
                all_matches.extend(matches)
            except Exception as e:
                logger.error("Error on PS=%s, cat=%s: %s", ps_id, cat, e)

        insert_rising_crimes(conn, all_matches)
        if conn:
            conn.close()

    except Exception as e:
        logger.critical("Script failed: %s", e, exc_info=True)
        if conn:
            conn.close()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
