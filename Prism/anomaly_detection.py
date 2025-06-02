"""
Anomaly Detection ETL Pipeline

This script implements an Extract-Transform-Load (ETL) pipeline to detect statistical anomalies
in crime reporting data, focusing on property-related crimes. It identifies police stations
with sudden spikes in incident reports using a statistical thresholding approach and stores
the results for further analysis.

Key Features:
1. **Statistical Anomaly Detection**:
   - Uses a 10-day moving window (excluding yesterday) to calculate baseline statistics
   - Flags incidents where yesterday's count exceeds (mean + 4σ) of the baseline window
   - Focuses on property-related crimes: Dacoity, Burglary, Robbery/Snatching, Vehicle Theft/Snatching

2. **Temporal Analysis**:
   - Analyzes 11-day historical window (10-day baseline + yesterday)
   - Maintains temporal ordering of cases for alert prioritization

3. **Crime Categories**:
   - Monitors Level3 case subcategories within 'Crime Against Property'
   - Identifies excess cases beyond expected statistical variation

4. **Database Integration**:
   - Connects to PostgreSQL for data extraction and anomaly storage
   - Implements idempotent inserts with conflict resolution

5. **Data Processing**:
   - Uses pandas for time-series analysis and statistical calculations
   - Applies pivoting for time window analysis
   - Implements efficient bulk inserts

6. **Logging**:
   - Maintains detailed log file in ./Logs/anomaly_detection.log
   - Tracks data volumes, processing steps, and findings

Execution Flow:
1. Establish PostgreSQL connection
2. Extract 11-day window of crime data
3. Calculate daily counts and statistical baselines
4. Identify anomalous police station/case type combinations
5. Retrieve specific excess cases for investigation
6. Store findings in anomaly_detection table

Statistical Methodology:
- Uses 4σ threshold (mean + 4 standard deviations) for anomaly detection
- Treats each (police_station_id, level3_case_nature) combination independently
- Accounts for zero-count days through explicit handling

Dependencies:
- PostgreSQL database with response_time table schema
- Python 3.8+ with pandas, numpy, and psycopg2
- Valid database credentials and connection parameters
- Directory structure with Logs/ subdirectory

The script is designed for automated execution to enable proactive identification of emerging
crime patterns requiring police attention. It complements existing rising crimes analysis
systems by focusing on sudden statistical deviations rather than multi-week trends.
"""

from datetime import date, timedelta, datetime
from collections import defaultdict
import psycopg2
import pandas as pd
import numpy as np
import logging
import os
import sys
from typing import List, Tuple, Dict, Any
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Utilities.utils import get_processed_db_connection


# --- Configuration Constants ---
DAYS_WINDOW = 10  # For mean and std calculation (excluding yesterday)
DAYS_LOOKBACK = DAYS_WINDOW + 1  # Include yesterday (10 days + yesterday)

# Configure logging (same as rising crimes)
log_dir = os.path.join(os.path.dirname(__file__), '..', 'Logs')
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, 'anomaly_detection.log')
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logging.info("Anomaly Detection ETL Started")


# --- Data Extraction (Reused from Rising Crimes) ---
def fetch_response_times(conn: psycopg2.extensions.connection, end_date: date) -> pd.DataFrame:
    """
    Fetch all 'Crime Against Property' calls for the 11-day window ending at end_date.
    """
    start_date = (end_date - timedelta(days=DAYS_LOOKBACK - 1)).strftime('%Y-%m-%d')
    sql = """
        SELECT *
        FROM response_time
        WHERE date BETWEEN %s AND %s
          AND level1_case_nature = 'Crime Against Property'
          AND level2_case_nature IN ('Dacoity','Burglary','Robbery/Snatching','Vehicle Theft','Vehicle Snatching') 
          AND police_station_id != 0 
          AND police_station IS NOT NULL
          AND parent_id = 0
    """
    df = pd.read_sql(sql, conn, params=(start_date, end_date.strftime('%Y-%m-%d')))
    # Convert date column to datetime.date
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df

# --- Anomaly Detection Calculations ---
def compute_anomaly_metrics(df: pd.DataFrame, end_date: date) -> pd.DataFrame:
    """
    Compute mean and std of daily counts for the last 10 days (excluding yesterday),
    and yesterday's count for each (police_station_id, level3_case_nature).
    Returns DataFrame with columns: police_station_id, level3_case_nature, mean_count,
    std_count, yesterday_count.
    """
    if df.empty:
        return pd.DataFrame(
            columns=["police_station_id", "level3_case_nature", "mean_count", "std_count", "yesterday_count"])

    # Define yesterday
    yesterday = end_date - timedelta(days=1)

    # Group by police_station_id, level3_case_nature, and date to get daily counts
    daily_counts = df.groupby(["police_station_id", "level3_case_nature", "date"]).size().reset_index(name="count")

    # Pivot to get counts per day as columns
    pivot = daily_counts.pivot_table(
        index=["police_station_id", "level3_case_nature"],
        columns="date",
        values="count",
        fill_value=0
    )

    # Sort dates
    all_dates = sorted(pivot.columns)

    # Check if we have enough data
    if len(all_dates) < DAYS_WINDOW:
        raise ValueError(f"Not enough days in data to compute metrics. Need at least {DAYS_WINDOW} days.")

    # Last 10 days (excluding yesterday)
    window_dates = [d for d in all_dates if d < yesterday]
    if len(window_dates) > DAYS_WINDOW:
        window_dates = window_dates[-DAYS_WINDOW:]  # Take the most recent 10 days before yesterday

    # Yesterday's counts
    yesterday_counts = pivot.get(yesterday, pd.Series(0, index=pivot.index))

    # Compute mean and std for the 10-day window
    window_counts = pivot[window_dates]
    mean_counts = window_counts.mean(axis=1)
    std_counts = window_counts.std(axis=1, ddof=1)  # ddof=1 for sample std

    # Create output DataFrame
    df_out = pd.DataFrame({
        "police_station_id": pivot.index.get_level_values(0),
        "level3_case_nature": pivot.index.get_level_values(1),
        "mean_count": mean_counts,
        "std_count": std_counts,
        "yesterday_count": yesterday_counts
    }).reset_index(drop=True)

    return df_out

# --- Identify Anomalies ---
def identify_anomalies(agg: pd.DataFrame) -> list[tuple[int, str, int]]:
    """
    Identify groups where yesterday's count > mean + 4 * std.
    Returns list of (police_station_id, level3_case_nature, excess_count).
    """
    anomalies = []
    for _, row in agg.iterrows():
        threshold = row["mean_count"] + 4 * row["std_count"]
        if row["yesterday_count"] > threshold:
            excess_count = int(row["yesterday_count"] - threshold)  # Excess cases
            if excess_count > 0:
                anomalies.append((row["police_station_id"], row["level3_case_nature"], excess_count))
    logging.info("Found %d groups with anomalies", len(anomalies))
    return anomalies


# --- Case Selection (Reused from Rising Crimes) ---
def fetch_excess_cases(
        conn: psycopg2.extensions.connection,
        ps: int,
        category: str,
        end_date: date,
        diff: int = None,
) -> pd.DataFrame:
    """
    Fetch  cases from yesterday for the given PS and level3_case_nature.
    If `diff` is None, returns all cases from yesterday.
    """
    yesterday = (end_date - timedelta(days=1)).strftime('%Y-%m-%d')
    sql = """
        SELECT *
        FROM response_time
        WHERE police_station_id = %s
          AND level3_case_nature = %s
          AND date = %s
        ORDER BY created_time ASC
    """
    params = [ps, category, yesterday]
    if diff is not None:
        sql += "\nLIMIT %s"
        params.append(diff)

    df = pd.read_sql(sql, conn, params=params)
    logging.info("Fetched %d cases for PS=%d, cat=%s (diff=%s)", len(df), ps, category, diff)
    return df


# --- Insert Anomalies ---
def insert_anomaly_cases(
        conn: psycopg2.extensions.connection,
        records: list[dict],
):
    """
    Bulk-insert into anomaly_detection. Assumes table exists with matching columns.
    """
    if not records:
        return

    df = pd.DataFrame(records)
    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    cols_sql = ", ".join(cols)
    insert_sql = f"""
        INSERT INTO anomaly_detection ({cols_sql})
        VALUES ({placeholders})
        ON CONFLICT (lead_id) DO NOTHING
    """

    with conn.cursor() as cur:
        data = [tuple(rec[col] for col in cols) for rec in records]
        cur.executemany(insert_sql, data)
    conn.commit()
    logging.info("Inserted %d records into anomaly_detection", len(records))


def create_anomaly_detection_table(conn: psycopg2.extensions.connection):
    """
    Inspect response_time schemas and build an empty anomaly_detection table.
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
            CREATE TABLE IF NOT EXISTS anomaly_detection (
                {', '.join(col_defs)},
                UNIQUE (lead_id)
            );
        """
        logging.info("Creating anomaly_detection table if not exists...")
        cur.execute(create_sql)
        conn.commit()
        logging.info("anomaly_detection table ready.")


def _col_def_sql(col_name: str, dtype: str, char_len: Any) -> str:
    """
    Helper to map all source columns to TEXT in the anomaly_detection table.
    """
    # Regardless of the source data_type, store everything as TEXT
    return f"{col_name} TEXT"

# --- Main Logic ---
def main():
    # Database connection

    with get_processed_db_connection() as conn:  # Replace with your connection details
        end_date = date.today()

        create_anomaly_detection_table(conn)

        # Fetch data for the last 11 days
        df = fetch_response_times(conn, end_date)

        # Compute anomaly metrics
        agg = compute_anomaly_metrics(df, end_date)

        # Identify anomalies
        anomalies = identify_anomalies(agg)

        # Fetch excess cases for anomalies
        anomaly_cases = []
        for ps, category, excess_count in anomalies:
            cases = fetch_excess_cases(conn, ps, category, end_date, diff=excess_count)
            anomaly_cases.extend(cases.to_dict('records'))

        # Insert into anomaly_detection
        insert_anomaly_cases(conn, anomaly_cases)


if __name__ == "__main__":
    main()
    logging.info("Anomaly Detection ETL Completed")

