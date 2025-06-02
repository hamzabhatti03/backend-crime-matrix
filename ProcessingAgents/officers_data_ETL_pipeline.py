"""
sync_officers.py

Load CNICs from an Excel file, fetch officer data from two external APIs,
and upsert into local PostgreSQL table `officer_registry`.

Usage:
    python sync_officers.py
"""
import sys
import os
import time
import random
import pandas as pd
import psycopg2
from psycopg2 import sql
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Utilities import utils, configs
#import utils
#import configs
from dotenv import load_dotenv
import logging
from datetime import datetime

load_dotenv()

log_dir = os.path.join(os.path.dirname(__file__), '..', 'Logs')
os.makedirs(log_dir, exist_ok=True)

log_path = os.path.join(log_dir, 'officer_sync.log')

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Log Execution start
logging.info("Sync Officers Data Execution Started")

# Adjust this path if you move the script
EXCEL_PATH = os.path.join(os.path.dirname(__file__), '..', 'DatabaseManager', 'nic_list.xlsx')
SHEET_NAME = 'Sheet1'

def get_db_connection():
    """
    Create and return a new PostgreSQL connection and cursor using environment vars.
    """
    conn = psycopg2.connect(
        dbname=os.getenv('LOGS_DB_NAME'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'),
        host=os.getenv('PG_HOST'),
        port=os.getenv('PG_PORT'),
    )
    cursor = conn.cursor()
    return conn, cursor

def create_officer_registry_table(cursor):
    """
    Create the officers_data table if it doesn't exist.
    """
    create_table_q = """
    CREATE TABLE IF NOT EXISTS officers_data (
        cnic VARCHAR PRIMARY KEY,
        designation_name TEXT,
        posting_district TEXT,
        ps_name_eng TEXT,
        last_synced TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    cursor.execute(create_table_q)

def upsert_officer(cursor, cnic, designation, district, ps_name):
    """
    Insert or update an officer record in officers_data.
    """
    upsert_q = """
    INSERT INTO officers_data (cnic, designation_name, posting_district, ps_name_eng)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (cnic) DO UPDATE
      SET designation_name = EXCLUDED.designation_name,
          posting_district = EXCLUDED.posting_district,
          ps_name_eng = EXCLUDED.ps_name_eng,
          last_synced = CURRENT_TIMESTAMP;
    """
    cursor.execute(upsert_q, (cnic, designation, district, ps_name))

def process_all_cnics():

    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET_NAME, dtype=str)
    if 'CNIC No.' not in df.columns:
        raise KeyError(f"'CNIC' column not found in sheet '{SHEET_NAME}' of {EXCEL_PATH}")
    cnics = df['CNIC No.'].dropna().unique()
    logging.info(f"Found {len(cnics)} unique CNICs to process.")

    conn, cursor = get_db_connection()
    create_officer_registry_table(cursor)
    conn.commit()

    # Loop through CNICs
    for cnic in cnics:
        try:
            officer_data = utils.fetch_officer_data(cnic)
            if not officer_data:
                logging.info(f"No data found for {cnic}. Skipping.")
                continue

            if 'exception' in officer_data:
                details = officer_data.get('original', {}).get('officer_details', {}) or {}
                # Officer Designation
                designation_raw = details.get("designation_name")
                designation = designation_raw.strip().lower() if isinstance(designation_raw, str) else None

                # Posting district
                posting_district_raw = details.get("posting_district") or ""
                district = None
                if posting_district_raw:
                    district = posting_district_raw.strip().lower()

                # Police station name: prefer 'temppolicestation', then 'ps_name_eng'
                # temp_ps = details.get("temppolicestation") or ""
                ps_eng_raw = details.get("ps_name_eng") or ""
                ps_candidate = ps_eng_raw # or temp_ps
                ps_name = ps_candidate.strip().lower() if ps_candidate else None
            else:
                # Designation
                designation_raw = officer_data.get("designation_name") or ""
                designation = designation_raw.strip().lower()

                # District
                district = officer_data.get("dst_name") or ""

                # PS name
                ps_eng_raw = officer_data.get("ps_name_eng") or ""
                ps_name = ps_eng_raw.strip().lower() if ps_eng_raw else None

            # Upsert into DB
            upsert_officer(cursor, cnic.strip(), designation, district, ps_name)
            conn.commit()

        except Exception as e:
            # Print error and continue with next CNIC
            logging.error(f"Error processing {cnic}: {e}")

        # Random delay between requests to avoid rate‑limiting
        delay = random.randint(1, 5)
        # print(f"    ⏳ Sleeping for {delay}s before next request...")
        time.sleep(delay)

    # 4. Cleanup
    cursor.close()
    conn.close()
    logging.info("✅ Sync Data completed.\n")

if __name__ == '__main__':
    process_all_cnics()
