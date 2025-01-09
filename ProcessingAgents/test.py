import mysql.connector
from Utilities.db_config import get_db_connection

def fetch_records(mysql_conn, table_name, limit=10):
    """Fetch and return records from the specified MySQL table."""
    try:
        cursor = mysql_conn.cursor(dictionary=True)
        query = f"SELECT * FROM {table_name} LIMIT %s"
        cursor.execute(query, (limit,))
        rows = cursor.fetchall()
        cursor.close()
        return rows
    except mysql.connector.Error as err:
        print(f"Error fetching records: {err}")
        return None

# Main test script
if __name__ == "__main__":
    # Establish MySQL connection
    mysql_conn = get_db_connection()
    if mysql_conn:
        # Specify the table name you want to test with
        table_name = '15_police_stations'  # Replace with your actual table name
        limit = 5  # Number of records to fetch for testing

        # Fetch records
        records = fetch_records(mysql_conn, table_name, limit)

        # Print fetched records for verification
        if records:
            print(f"Fetched {len(records)} records from {table_name}:")
            for record in records:
                print(record)
        else:
            print("No records found or an error occurred.")

        # Close the connection
        mysql_conn.close()
    else:
        print("Failed to connect to MySQL.")