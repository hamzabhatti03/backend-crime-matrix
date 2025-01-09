import requests
from Utilities import utils
from Utilities import configs
from datetime import datetime, timedelta
import time
import sqlite3


def initialize_database():
    """Create the database and table if not exists."""
    conn = utils.get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fir_data (
            date TEXT,
            district_id INTEGER,
            district TEXT,
            police_station INTEGER,
            fir_count INTEGER,
            PRIMARY KEY (district_id, police_station, date)
        )
    """)
    conn.commit()
    conn.close()


def fetch_fir_data(district_id, date):
    """Fetches FIR data for a specific district and date."""
    url = f"https://police15.psca.gop.pk/public/fir/police-stations"
    params = {
        "district_id": district_id,
        "start_date": date,
        "end_date": date
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for district {district_id} on {date}: {response.status_code}")
        return []


def store_data_to_db(data, district_id, date, max_retries=5, retry_delay=5):
    """Stores fetched FIR data into the SQLite database with retry logic."""
    retries = 0

    while retries < max_retries:
        try:
            conn = utils.get_db_connection()
            cursor = conn.cursor()

            for item in data:
                cursor.execute("""
                    INSERT OR REPLACE INTO fir_data (district_id, district, date, police_station, fir_count)
                    VALUES (?, ?, ?, ?, ?)
                """, (
                    district_id,
                    configs.DISTRICTS_DICTIONARY[district_id],
                    date,
                    item['psca_ps_name'],
                    item['fir_count']
                ))

            conn.commit()
            conn.close()
            return  # Exit the function if data is successfully stored

        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                retries += 1
                print(f"Database is locked. Retrying {retries}/{max_retries} in {retry_delay} seconds...")
                time.sleep(retry_delay)


def fetch_and_store_all_data(start_date, end_date):
    """Fetches and stores FIR data for all districts and dates in the range."""
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.strptime(end_date, "%Y-%m-%d")

    while True:
        # Determine the range of dates to iterate
        if current_date > end_date:
            # Shift to the last 10 days
            current_date = datetime.now() - timedelta(days=10)
            end_date = datetime.now()

        # Iterate through the range of dates
        while current_date <= end_date:
            for district_id, district_name in configs.DISTRICTS_MAPPING:
                print(f"Fetching data for district {district_name} on {current_date.strftime('%Y-%m-%d')}...")
                data = fetch_fir_data(district_id, current_date.strftime("%Y-%m-%d"))
                store_data_to_db(data, district_id, current_date.strftime("%Y-%m-%d"))
            print("waiting for 25 seconds before fetching data for next date...")
            time.sleep(25)
            current_date += timedelta(days=1)

        # Sleep for 5 minutes before updating the last 10 days
        print("Waiting before fetching the latest data...")
        time.sleep(300)


# Run the script
if __name__ == '__main__':
    start_date = "2024-12-25"
    end_date = "2025-01-07"
    initialize_database()
    fetch_and_store_all_data(start_date, end_date)
