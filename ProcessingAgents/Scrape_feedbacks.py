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
        CREATE TABLE IF NOT EXISTS feedback_stats (
            created_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
            accepted INTEGER,
            dispatched TEXT,
            feedback INTEGER,
            reopen INTEGER,
            closed INTEGER,
            caller_feedback INTEGER
        )
    """)

    cursor.execute("""
            CREATE TABLE IF NOT EXISTS dist_feedback_count (
                created_at TIMESTAMP DEFAULT (datetime('now', 'localtime')),
                district TEXT,
                positive INTEGER,
                not_responding INTEGER,
                negative INTEGER,
                PRIMARY KEY (district)
            )
        """)

    conn.commit()
    conn.close()

def fetch_and_store_feedback_data():
    """Fetch data from APIs and store it in the database."""
    # Load session
    session = utils.load_vehicle_session()

    # Fetch data from the APIs
    try:
        status_response = session.get(configs.FEEDBACK_15_STATS_URL)
        status_response.raise_for_status()

        # Parse the JSON response (single object)
        status_data = status_response.json()

        # Ensure the response is a dictionary
        if not isinstance(status_data, dict):
            print("Unexpected API response format. Expected a dictionary.")
            return
    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return
    except ValueError:
        print("Error decoding JSON from API")
        return

        # Insert data into the database
    try:
        conn = utils.get_db_connection()
        cursor = conn.cursor()

        # Replace data in feedback_stats table
        cursor.execute("""
                INSERT OR REPLACE INTO feedback_stats (
                    accepted, dispatched, feedback, reopen, closed, caller_feedback
                ) VALUES (
                    :accepted, :dispatched, :feedback, :reopen, :closed, :caller_feedback
                )
            """, {
            'accepted': int(status_data.get('accepted', 0)),
            'dispatched': int(status_data.get('dispatched', 0)),
            'feedback': int(status_data.get('feedback', 0)),
            'reopen': int(status_data.get('reopen', 0)),
            'closed': int(status_data.get('closed', 0)),
            'caller_feedback': int(status_data.get('caller_feedback', 0))
        })

        conn.commit()
        conn.close()

    except sqlite3.Error as e:
        print(f"[{datetime.now()}] Database error: {e}")

def fetch_and_store_dist_feedback_count():
    """Fetch data from the dist_feedback_count API and store it in the database."""
    session = utils.load_vehicle_session()

    try:
        # Fetch the response
        dist_response = session.get(configs.FEEDBACK_DISTRICT_COUNT_URL)
        dist_response.raise_for_status()

        # Parse the JSON response
        data = dist_response.json()

        # Validate the structure of the response
        if isinstance(data, list):  # If the response is a list of dictionaries
            records = data
        elif isinstance(data, dict):  # If the response is a single dictionary
            records = [data]  # Wrap it in a list for uniform processing
        else:
            print("Unexpected API response format. Neither list nor dictionary.")
            return

        # Insert data into the database
        conn = utils.get_db_connection()
        cursor = conn.cursor()

        records = data.get('data',[])
        for record in records:
            # Validate that the record is a dictionary
            if not isinstance(record, dict):
                print(f"Unexpected record format: {record}")
                continue

            # Insert or replace data
            cursor.execute(
                """
                INSERT OR REPLACE INTO dist_feedback_count (
                    district, positive, not_responding, negative
                ) VALUES (:district, :positive, :not_responding, :negative)
                """,
                {
                    'district': record.get("District", 0),
                    'positive': int(record.get("Positive", 0)),
                    'not_responding': int(record.get("Not-Responding", 0)),
                    'negative': int(record.get("Negative", 0)),
                }
            )

        conn.commit()
        conn.close()
        print("District feedback count successfully updated.")

    except requests.RequestException as e:
        print(f"Error fetching district feedback count: {e}")
    except sqlite3.Error as e:
        print(f"[{datetime.now()}] Database error: {e}")
    except ValueError as e:
        print(f"Error processing API response: {e}")

def main():
    """Main function to initialize database and fetch data iteratively."""
    initialize_database()

    while True:
        print("Fetching and storing data...")
        fetch_and_store_feedback_data()
        fetch_and_store_dist_feedback_count()

        print("Data updated. Waiting for the next iteration...")
        time.sleep(180)

if __name__ == "__main__":
    main()
