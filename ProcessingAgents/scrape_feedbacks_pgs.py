import requests
from Utilities import utils
from Utilities import configs
from datetime import datetime
import time
import psycopg2


def initialize_database():
    """Create the database and table if not exists."""
    conn = utils.get_processed_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS feedback_stats (
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            district TEXT PRIMARY KEY,
            positive INTEGER,
            not_responding INTEGER,
            negative INTEGER
        )
    """)

    conn.commit()
    conn.close()


def fetch_and_store_feedback_data():
    """Fetch data from APIs and store it in the database."""
    session = utils.load_vehicle_session()

    try:
        # Fetch data from the API
        status_response = session.get(configs.FEEDBACK_15_STATS_URL)
        status_response.raise_for_status()
        status_data = status_response.json()

        if not isinstance(status_data, dict):
            print("Unexpected API response format. Expected a dictionary.")
            return

    except requests.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return
    except ValueError:
        print("Error decoding JSON from API")
        return

    try:
        conn = utils.get_processed_db_connection()
        cursor = conn.cursor()

        # Replace data in feedback_stats table
        cursor.execute("""
            INSERT INTO feedback_stats (
                accepted, dispatched, feedback, reopen, closed, caller_feedback, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (created_at)
            DO UPDATE SET
                accepted = EXCLUDED.accepted,
                dispatched = EXCLUDED.dispatched,
                feedback = EXCLUDED.feedback,
                reopen = EXCLUDED.reopen,
                closed = EXCLUDED.closed,
                caller_feedback = EXCLUDED.caller_feedback;
        """, (
            int(status_data.get('accepted', 0)),
            int(status_data.get('dispatched', 0)),
            int(status_data.get('feedback', 0)),
            int(status_data.get('reopen', 0)),
            int(status_data.get('closed', 0)),
            int(status_data.get('caller_feedback', 0))
        ))

        conn.commit()
        conn.close()

    except psycopg2.Error as e:
        print(f"[{datetime.now()}] Database error: {e}")


def fetch_and_store_dist_feedback_count():
    """Fetch data from the dist_feedback_count API and store it in the database."""
    session = utils.load_vehicle_session()

    try:
        # Fetch the response
        dist_response = session.get(configs.FEEDBACK_DISTRICT_COUNT_URL)
        dist_response.raise_for_status()
        data = dist_response.json()

        if isinstance(data, dict):
            records = data.get('data', [])
        elif isinstance(data, list):
            records = data
        else:
            print("Unexpected API response format. Neither list nor dictionary.")
            return

        conn = utils.get_processed_db_connection()
        cursor = conn.cursor()

        for record in records:
            if not isinstance(record, dict):
                print(f"Unexpected record format: {record}")
                continue

            # Insert or update data in the table
            cursor.execute("""
                INSERT INTO dist_feedback_count (
                    district, positive, not_responding, negative, created_at
                )
                VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (district)
                DO UPDATE SET
                    positive = EXCLUDED.positive,
                    not_responding = EXCLUDED.not_responding,
                    negative = EXCLUDED.negative,
                    created_at = CURRENT_TIMESTAMP;
            """, (
                record.get("District", "").strip(),
                int(record.get("Positive", 0)),
                int(record.get("Not-Responding", 0)),
                int(record.get("Negative", 0))
            ))

        conn.commit()
        conn.close()

        print("District feedback count successfully updated.")

    except requests.RequestException as e:
        print(f"Error fetching district feedback count: {e}")
    except psycopg2.Error as e:
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
        # time.sleep(180)


if __name__ == "__main__":
    main()
