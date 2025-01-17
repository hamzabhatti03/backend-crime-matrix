import pickle
from datetime import datetime, timedelta
from Utilities import configs, utils
import psycopg2
from psycopg2.extras import execute_values


def sanitize_datetime(value):
    """Sanitize datetime fields to ensure compatibility with PostgreSQL."""
    try:
        if value in ["0000-00-00 00:00:00", None, ""]:
            return None  # Return NULL for invalid dates
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None  # Handle any other invalid formats


def load_vehicle_session():
    """Load or create a vehicle session."""
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
    """Create the database and police_mv_locations table if it does not exist."""
    conn = utils.get_processed_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS police_mv_locations (
            id SERIAL PRIMARY KEY,
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
            created_at TIMESTAMP,
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
            updated_at TIMESTAMP,
            district TEXT
        )
    """)

    conn.commit()
    conn.close()


def insert_vehicle_data(data):
    """Insert vehicle data into the database with sanitized datetime fields."""
    conn = utils.get_processed_db_connection()
    cursor = conn.cursor()

    vehicle_records = data.get('liveLocations', [])
    if not vehicle_records:
        print(f"[{datetime.now()}] No vehicle records to insert.")
        return

    try:
        query = """
            INSERT INTO police_mv_locations (
                id, device_no, user_id, name, phone_number, cnic, latitude, longitude, 
                vehicle_type, district_id, police_station, created_at, status, 
                registration_no, tracker_id, is_tracker, tracker_company, tracker_address, 
                vehicle_name, vehicle_ps, vehicle_district, unit_type, updated_at, district
            ) VALUES %s
            ON CONFLICT (id) DO UPDATE SET
                device_no = EXCLUDED.device_no,
                user_id = EXCLUDED.user_id,
                name = EXCLUDED.name,
                phone_number = EXCLUDED.phone_number,
                cnic = EXCLUDED.cnic,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                vehicle_type = EXCLUDED.vehicle_type,
                district_id = EXCLUDED.district_id,
                police_station = EXCLUDED.police_station,
                created_at = EXCLUDED.created_at,
                status = EXCLUDED.status,
                registration_no = EXCLUDED.registration_no,
                tracker_id = EXCLUDED.tracker_id,
                is_tracker = EXCLUDED.is_tracker,
                tracker_company = EXCLUDED.tracker_company,
                tracker_address = EXCLUDED.tracker_address,
                vehicle_name = EXCLUDED.vehicle_name,
                vehicle_ps = EXCLUDED.vehicle_ps,
                vehicle_district = EXCLUDED.vehicle_district,
                unit_type = EXCLUDED.unit_type,
                updated_at = EXCLUDED.updated_at,
                district = EXCLUDED.district;
        """

        records_to_insert = [(
            record.get('id'),
            record.get('device_no'),
            record.get('user_id'),
            record.get('name'),
            record.get('phone_number'),
            record.get('cnic'),
            record.get('latitude'),
            record.get('longitude'),
            record.get('vehicle_type'),
            record.get('district_id'),
            record.get('police_station'),
            sanitize_datetime(record.get('created_at')),
            record.get('status'),
            record.get('registration_no'),
            record.get('tracker_id'),
            record.get('is_tracker'),
            record.get('tracker_company'),
            record.get('tracker_address'),
            record.get('vehicle_name'),
            record.get('vehicle_ps'),
            record.get('vehicle_district'),
            record.get('unit_type'),
            sanitize_datetime(record.get('updated_at')),
            record.get('district')
        ) for record in vehicle_records]

        execute_values(cursor, query, records_to_insert)
        conn.commit()
        print(f"[{datetime.now()}] Data successfully inserted into the database.")
    except psycopg2.Error as e:
        print(f"[{datetime.now()}] Database error: {e}")
    except Exception as e:
        print(f"[{datetime.now()}] Error occurred: {e}")
    finally:
        conn.close()


def fetch_and_store_vehicle_data():
    """Fetch vehicle data from the API and store it in the database."""
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


def main():
    create_table()
    fetch_and_store_vehicle_data()


# Uncomment the line below to run the script
# main()
