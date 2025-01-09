import time
from datetime import datetime
from Utilities import utils


def initialize_database():
    """Create the database and table if not exists."""
    conn = utils.get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            busy_count INTEGER,
            busy_bikes INTEGER,
            busy_vans INTEGER,
            offline_count INTEGER,
            offline_bikes INTEGER,
            offline_vans INTEGER,
            online_count INTEGER,
            online_bikes INTEGER,
            online_vans INTEGER,
            total_count INTEGER,
            total_bikes INTEGER,
            total_vans INTEGER
        )
    """)
    conn.commit()
    conn.close()



def insert_vehicle_stats(data):
    """Insert the fetched vehicle stats into the database."""
    conn = utils.get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO vehicle_stats (
            busy_count, busy_bikes, busy_vans,
            offline_count, offline_bikes, offline_vans,
            online_count, online_bikes, online_vans,
            total_count, total_bikes, total_vans
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data['Busy']['count'], data['Busy']['Bikes'], data['Busy']['Vans'],
        data['Offline']['count'], data['Offline']['Bikes'], data['Offline']['Vans'],
        data['Online']['count'], data['Online']['Bikes'], data['Online']['Vans'],
        data['Total']['count'], data['Total']['Bikes'], data['Total']['Vans']
    ))
    conn.commit()
    conn.close()


def fetch_vehicle_stats_and_store(session):
    """Fetch vehicle stats and store them in the database."""
    data = utils.fetch_vehicle_stats(session)
    if "error" in data:
        print(f"Error fetching data: {data['error']}")
    else:
        insert_vehicle_stats(data)
        print(f"Data inserted into DB at {datetime.now()}")



def main():
    # Initialize database
    initialize_database()

    # Load or create a vehicle session
    session = utils.load_vehicle_session()

    if session is None:
        session = utils.create_vehicle_session()
        utils.save_vehicle_session(session)

    # Run the loop to fetch and store data every 90 seconds
    while True:
        try:
            fetch_vehicle_stats_and_store(session)
        except Exception as e:
            print(f"An error occurred: {e}")
        time.sleep(90)

if __name__ == "__main__":
    main()

