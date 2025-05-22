import sys
import os
import base64
import requests
from datetime import datetime, timedelta, date
import time
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Utilities.utils import get_processed_db_connection
from Utilities.configs import PKM_DISTRICT_MAPPING
import logging

# Configure logging
log_dir = os.path.join(os.path.dirname(__file__), '..', 'Logs')
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, 'pkm_data.log')
logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Log Execution start
logging.info("PKM Data ETL Started")

# API authentication
token = os.getenv('PKM_API_TOKEN', '').strip()
user = os.getenv('PKM_USERNAME', '').strip()
pwd = os.getenv('PKM_PASSWORD', '').strip()
basic_token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
headers = {
    'sec_key': token,
    'Authorization': f"Basic {basic_token}",
}

# Define date range
today = date.today()
start_date = today - timedelta(days=19)
yesterday = today - timedelta(days=1)
date_list = [start_date + timedelta(days=x) for x in range((yesterday - start_date).days + 1)]

# Establish database connection
conn = get_processed_db_connection()
cursor = conn.cursor()

def create_table(conn, cursor):
    """Create the pkm_data table if it doesn't exist."""
    query = """
    CREATE TABLE IF NOT EXISTS pkm_data (
        district_id INT,
        district_name VARCHAR(50),
        date DATE,
        service_name VARCHAR(50),
        pending INT,
        complete INT,
        inprogress INT,
        PRIMARY KEY (district_id, date, service_name)
    );
    """
    try:
        cursor.execute(query)
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"Failed to create table: {e}")
        raise  # Re-raise to exit the script after cleanup

def fetch_and_store_data(date_str, district_id, district_name):
    """Fetch data from the API and store it in the database."""
    try:
        payload = {
            'center_type_id': (None, "0"),
            'district_id': (None, str(district_id)),
            'date_from': (None, date_str),
            'date_to': (None, date_str),
        }
        resp = requests.post(os.getenv('PKM_URL'), headers=headers, files=payload)
        resp.raise_for_status()  # Raises an exception for HTTP errors
        body = resp.json()
        if not body.get('status'):
            logging.error(f"⚠️ PKM API returned error for district {district_id} on {date_str}: {body.get('message')}")
            return

        data = body.get('data', {})
        # Fix date parsing to match the format from strftime('%d-%m-%Y')
        date_obj = datetime.strptime(date_str, '%d-%m-%Y').date()
        for service, stats in data.items():
            cursor.execute("""
            INSERT INTO pkm_data (district_id, district_name, date, service_name, pending, complete, inprogress)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (district_id, date, service_name)
            DO UPDATE SET
                pending = EXCLUDED.pending,
                complete = EXCLUDED.complete,
                inprogress = EXCLUDED.inprogress;
            """, (district_id, district_name, date_obj, service, stats['payment_pending'], stats['complete'], stats['inprogress']))
        conn.commit()
    except requests.RequestException as e:
        logging.error(f"API request failed for district {district_id} on {date_str}: {e}")
        conn.rollback()
    except ValueError as e:
        logging.error(f"JSON parsing failed for district {district_id} on {date_str}: {e}")
        conn.rollback()
    except Exception as e:
        logging.error(f"Unexpected error for district {district_id} on {date_str}: {e}")
        conn.rollback()

# Main execution
try:
    # Create the table before processing data
    create_table(conn, cursor)

    districts = list(PKM_DISTRICT_MAPPING.items())
    for current_date in date_list:
        date_str = current_date.strftime('%d-%m-%Y')
        logging.info(f"Processing data for {date_str}")
        # Process districts in batches of 6
        time.sleep(60)
        logging.info("Waiting 60 seconds before starting a new date due to API rate limit...")
        for i in range(0, len(districts), 6):
            batch = districts[i:i + 6]
            for district_name, district_id in batch:
                fetch_and_store_data(date_str, district_id, district_name)
            # Wait 60 seconds if there are more districts to process
            if i + 6 < len(districts):
                logging.info("Waiting 60 seconds due to API rate limit...")
                time.sleep(60)
finally:
    # Ensure resources are cleaned up
    cursor.close()
    conn.close()
    logging.info("Database connection closed.")