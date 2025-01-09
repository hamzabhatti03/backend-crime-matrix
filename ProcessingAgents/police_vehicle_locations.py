import pickle
from datetime import datetime
from Utilities import configs
from Utilities import utils
import sqlite3
import time

def load_vehicle_session():
    try:
        with open(configs.SESSION_VEHICLE_FILE, 'rb') as f:
            data = pickle.load(f)
            session = data['session']
            last_validation = data['last_validation']

            if datetime.now() - last_validation > configs.SESSION_EXPIRY_TIME:
                print("Session expired. Creating a new one.")
                session = utils.create_vehicle_session()
                utils.save_vehicle_session(session)

            return session
    except FileNotFoundError:
        print("No saved session found. Creating a new one.")
        session = utils.create_vehicle_session()
        utils.save_vehicle_session(session)
        return session


def create_table():
    """Create the database and vehicle_data table if it does not exist."""
    conn = utils.get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS police_mv_locations (
            id INTEGER PRIMARY KEY,
            device_no TEXT,
            user_id INTEGER,
            name TEXT,
            phone_number TEXT,
            cnic TEXT,
            latitude REAL,
            longitude REAL,
            vehicle_type INTEGER,
            district_id INTEGER,
            police_station TEXT,
            created_at TEXT,
            status TEXT,
            registration_no TEXT,
            tracker_id TEXT,
            is_tracker TEXT,
            tracker_company TEXT,
            tracker_address TEXT,
            vehicle_name TEXT,
            vehicle_ps TEXT,
            vehicle_district TEXT,
            unit_type TEXT,
            updated_at TEXT,
            district TEXT
        )
    """)

    conn.commit()
    conn.close()



def insert_vehicle_data(data):

    conn = utils.get_db_connection()
    cursor = conn.cursor()
    try:
        vehicle_records = data.get('liveLocations', [])
        for record in vehicle_records:
            cursor.execute("""
                INSERT OR REPLACE INTO police_mv_locations (
                    id, device_no, user_id, name, phone_number, cnic,
                    latitude, longitude, vehicle_type, district_id, police_station,
                    created_at, status, registration_no, tracker_id, is_tracker,
                    tracker_company, tracker_address, vehicle_name, vehicle_ps,
                    vehicle_district, unit_type, updated_at, district
                ) VALUES (
                    :id, :device_no, :user_id, :name, :phone_number, :cnic,
                    :latitude, :longitude, :vehicle_type, :district_id, :police_station,
                    :created_at, :status, :registration_no, :tracker_id, :is_tracker,
                    :tracker_company, :tracker_address, :vehicle_name, :vehicle_ps,
                    :vehicle_district, :unit_type, :updated_at, :district
                )
            """, record)

        conn.commit()
        print(f"[{datetime.now()}] Data successfully inserted into the database.")
    except sqlite3.Error as e:
        print(f"[{datetime.now()}] Database error: {e}")
    except Exception as e:
        print(f"[{datetime.now()}] Error occurred: {e}")
    finally:
        conn.close()


def fetch_and_store_vehicle_data():

    while True:
        try:
            session = load_vehicle_session()
            response = session.get(configs.LIVE_VEHICLE_DATA)

            if response.status_code == 200:
                data = response.json()
                if data:
                    insert_vehicle_data(data)
                else:
                    print(f"[{datetime.now()}] No data received from the API.")
            else:
                print(f"[{datetime.now()}] Failed to fetch data: {response.status_code}, {response.text}")
        except Exception as e:
            print(f"[{datetime.now()}] Error occurred: {e}")

        print("Waiting for 1.5 minutes before the next fetch...")
        time.sleep(90)




# def main():
if __name__ == '__main__':
    create_table()
    fetch_and_store_vehicle_data()