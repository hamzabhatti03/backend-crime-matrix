import psycopg2
from psycopg2 import sql

# Database connection parameters
db_config = {
    "dbname": "notifications_aicm",  # Name of the database
    "user": "postgres",         # Your PostgreSQL username
    "password": "psca@officialmai1",     # Your PostgreSQL password
    "host": "10.20.170.151",             # Host where PostgreSQL is running
    "port": 5432                   # Default PostgreSQL port
}

# SQL query to create the table
create_table_query = """
CREATE TABLE IF NOT EXISTS realtime_notifications (
    case_number VARCHAR(50) NOT NULL,
    user_name VARCHAR(100) NOT NULL,
    update_from TEXT NOT NULL,
    type TEXT NOT NULL,
    PRIMARY KEY (case_number, user_name)
);
"""

try:
    # Connect to the PostgreSQL database
    connection = psycopg2.connect(**db_config)
    cursor = connection.cursor()

    # Execute the CREATE TABLE query
    cursor.execute(create_table_query)

    # Commit the transaction
    connection.commit()

    print("Table 'realtime_notifications' created successfully.")

except psycopg2.Error as e:
    print(f"An error occurred: {e}")

finally:
    # Close the cursor and connection
    if cursor:
        cursor.close()
    if connection:
        connection.close()