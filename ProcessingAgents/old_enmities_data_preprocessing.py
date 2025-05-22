"""
load_old_enmities.py

Reads an Excel file with Urdu column names, maps columns to English, and upserts into
the `old_enmities` Postgres table without overwriting existing rows.
"""

import os
import logging
import pandas as pd
from psycopg2 import sql
from psycopg2.extras import execute_values
from Utilities.utils import get_processed_db_connection
import numpy as np

# 1) Mapping from Urdu to English
COLUMN_MAPPING = {
    "طول": "Longitude",
    "عرض": "Latitude",
    "متاثرہ اشخاس": "Victims",
    "مقاصد": "Motives",
    "ملزمان": "Suspects",
    "کرائم سب ہیڈ": "Crime Sub-Head",
    "کرائم ہیڈ": "Crime Head",
    "پوزیشن": "Position",
    "جرم": "Crime Section",
    "ایف آئی آر نمبر": "FIR Number",
    "تھانہ": "Police Station",
    "ضلع": "District",
    "نمبر": "Serial Number",
}

# 2) Path to your Excel file (adjust if needed)
current_dir = os.path.dirname(os.path.abspath(__file__))

# The project root is assumed to be one level up from the current directory
project_root = os.path.join(current_dir, '..')

# Define the path to the 'fir_data' folder located in the project root
old_enmities_folder = os.path.join(project_root, 'DatabaseManager')
EXCEL_PATH = os.path.join(old_enmities_folder, "old_enmities.xlsx")

# 3) Target table name
TABLE_NAME = "old_enmities"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def snake_case(name: str) -> str:
    """Convert a human-readable name to snake_case."""
    return (
        name.strip()
        .lower()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(".", "")
    )

def pg_type_from_dtype(dtype) -> str:
    """Map a pandas dtype to a matching Postgres type."""
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    if pd.api.types.is_float_dtype(dtype):
        return "DOUBLE PRECISION"
    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    if pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP WITHOUT TIME ZONE"
    # fallback to text for anything else
    return "TEXT"


def normalize_row(row):
    normalized = []
    for val in row:
        if isinstance(val, np.generic):
            # convert any np scalar (int64, float64, etc.) to Python type
            normalized.append(val.item())
        else:
            normalized.append(val)
    return tuple(normalized)

def main():
    logging.info("Reading Excel file from %s", EXCEL_PATH)
    df = pd.read_excel(EXCEL_PATH, engine='openpyxl')

    # 1) Rename Urdu → English
    df = df.rename(columns=COLUMN_MAPPING)

    # 2) Normalize to snake_case columns
    df.columns = [snake_case(c) for c in df.columns]

    df = df.astype(object)


    # 3) Connect to the database
    conn = get_processed_db_connection()
    cursor = conn.cursor()

    # 4) Build CREATE TABLE statement
    cols_defs = []
    for col, dtype in zip(df.columns, df.dtypes):
        pg_type = pg_type_from_dtype(dtype)
        cols_defs.append(sql.SQL("{} {}").format(sql.Identifier(col), sql.SQL(pg_type)))

    # add unique constraint on (fir_number, police_station)
    create_table = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            {cols},
            UNIQUE (fir_number, police_station)
        );
    """).format(
        table=sql.Identifier(TABLE_NAME),
        cols=sql.SQL(", ").join(cols_defs),
    )

    logging.info("Ensuring table %s exists", TABLE_NAME)
    cursor.execute(create_table)
    conn.commit()

    # 5) Prepare INSERT ... ON CONFLICT DO NOTHING
    columns_sql = sql.SQL(", ").join(map(sql.Identifier, df.columns))
    placeholders = sql.SQL(", ").join(sql.Placeholder() * len(df.columns))
    insert_sql = sql.SQL("""
        INSERT INTO {table} ({cols})
        VALUES %s
        ON CONFLICT (fir_number, police_station) DO NOTHING
    """).format(
        table=sql.Identifier(TABLE_NAME),
        cols=columns_sql,
    )

    # Convert DataFrame to list of tuples
    records = df.where(pd.notnull(df), None).to_records(index=False)
    values = [tuple(row) for row in records]

    logging.info("Inserting %d records (skipping existing)...", len(values))
    execute_values(cursor, insert_sql.as_string(conn), values, template=None, page_size=100)

    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Done.")


if __name__ == "__main__":
    main()
