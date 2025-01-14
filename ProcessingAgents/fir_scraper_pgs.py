import requests
from Utilities import utils
from Utilities import configs
from datetime import datetime, timedelta
import time
import psycopg2
from psycopg2 import sql


def initialize_database():
    """Create the database and table if not exists."""
    with utils.get_processed_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fir_data (
                    date TEXT,
                    district_id INTEGER,
                    district TEXT,
                    police_station TEXT,
                    fir_count INTEGER,
                    PRIMARY KEY (district_id, police_station, date)
                )
            """)
            conn.commit()


def fetch_fir_data(district_id, date):
    """Fetches FIR data for a specific district and date."""
    url = "https://police15.psca.gop.pk/public/fir/police-stations"
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
    """Stores fetched FIR data into the PostgreSQL database with retry logic."""
    retries = 0

    while retries < max_retries:
        try:
            with utils.get_processed_db_connection() as conn:
                with conn.cursor() as cursor:
                    for item in data:
                        cursor.execute("""
                            INSERT INTO fir_data (district_id, district, date, police_station, fir_count)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (district_id, police_station, date)
                            DO UPDATE SET
                                fir_count = EXCLUDED.fir_count
                        """, (
                            district_id,
                            configs.DISTRICTS_DICTIONARY[district_id],
                            date,
                            item['psca_ps_name'],
                            item['fir_count']
                        ))
                    conn.commit()
            return  # Exit the function if data is successfully stored

        except psycopg2.OperationalError as e:
            if "could not connect to server" in str(e):
                retries += 1
                print(f"Database connection error. Retrying {retries}/{max_retries} in {retry_delay} seconds...")
                time.sleep(retry_delay)


def fetch_and_store_all_data(start_date):
    """Fetches and stores FIR data for all districts and dates in the range."""
    current_date = datetime.strptime(start_date, "%Y-%m-%d")
    date_str = current_date.strftime("%Y-%m-%d")
    for district_id, district_name in configs.DISTRICTS_MAPPING:
        print(f"Fetching data for district {district_name} on {date_str}...")
        data = fetch_fir_data(district_id, date_str)
        store_data_to_db(data, district_id, date_str)
    print("Waiting for 25 seconds before fetching data for the next date...")


def main(start_date):
    initialize_database()
    fetch_and_store_all_data(start_date)


main("2025-01-06")
