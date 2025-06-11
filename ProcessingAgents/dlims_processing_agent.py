import os
import requests
import sys
import base64
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Utilities.utils import get_processed_db_connection

# Load environment variables
load_dotenv()

# logging
log_dir = os.path.join(os.path.dirname(__file__), '..', 'Logs')
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, 'dlims_data.log')

logging.basicConfig(
    filename=log_path,
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Log script start
logging.info("DLIMS Data ETL Started")

# Date range for a week's data
today = datetime.now().date()
start_date = today - timedelta(days=7)
end_date = today - timedelta(days=1)
date_list = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]


# District mapping
district_mapping = {
    "BAHAWALNAGAR": "30101",
    "BAHAWALPUR": "30102",
    "RAHIM YAR KHAN": "30103",
    "D G KHAN": "30201",
    "LAYYAH": "30202",
    "MUZAFFARGARH": "30203",
    "RAJANPUR": "30204",
    "FAISALABAD": "30301",
    "JHANG": "30302",
    "TOBA TEK SINGH": "30303",
    "CHINIOT": "30304",
    "GUJRANWALA": "30401",
    "GUJRAT": "30402",
    "SIALKOT": "30403",
    "HAFIZABAD": "30404",
    "MANDI BAHAUDDIN": "30405",
    "NAROWAL": "30406",
    "KASUR": "30501",
    "LAHORE": "30502",
    "OKARA": "30503",
    "SHEIKHUPURA": "30504",
    "NANKANA SAHIB": "30505",
    "MULTAN": "30601",
    "SAHIWAL": "30602",
    "VEHARI": "30603",
    "KHANEWAL": "30604",
    "PAKPATTAN": "30605",
    "LODHRAN": "30606",
    "ATTOCK": "30701",
    "JHELUM": "30702",
    "RAWALPINDI": "30703",
    "CHAKWAL": "30704",
    "MURREE": "30705",
    "BHAKHAR": "30801",
    "KHUSHAB": "30802",
    "MIANWALI": "30803",
    "SARGODHA": "30804"
}


district_info = {
    "BAHAWALNAGAR": (38, "Bahawalnagar"),
    "BAHAWALPUR": (37, "Bahawalpur"),
    "RAHIM YAR KHAN": (39, "Rahimyar Khan"),
    "D G KHAN": (33, "D.G. Khan"),
    "LAYYAH": (36, "Layyah"),
    "MUZAFFARGARH": (35, "Muzaffargarh"),
    "RAJANPUR": (34, "Rajanpur"),
    "FAISALABAD": (20, "Faisalabad"),
    "JHANG": (22, "Jhang"),
    "TOBA TEK SINGH": (24, "T.T. Singh"),
    "CHINIOT": (23, "Chiniot"),
    "GUJRANWALA": (4, "Gujranwala"),
    "GUJRAT": (7, "Gujrat"),
    "SIALKOT": (9, "Sialkot"),
    "HAFIZABAD": (6, "Hafizabad"),
    "MANDI BAHAUDDIN": (8, "M.B. Din"),
    "NAROWAL": (10, "Narowal"),
    "KASUR": (3, "Kasur"),
    "LAHORE": (40, "Lahore"),
    "OKARA": (31, "Okara"),
    "SHEIKHUPURA": (1, "Sheikhupura"),
    "NANKANA SAHIB": (2, "Nankana Sb"),
    "MULTAN": (25, "Multan"),
    "SAHIWAL": (30, "Sahiwal"),
    "VEHARI": (29, "Vehari"),
    "KHANEWAL": (28, "Khanewal"),
    "PAKPATTAN": (32, "Pakpattan"),
    "LODHRAN": (27, "Lodhran"),
    "ATTOCK": (13, "Attock"),
    "JHELUM": (14, "Jhelum"),
    "RAWALPINDI": (11, "Rawalpindi"),
    "CHAKWAL": (15, "Chakwal"),
    "MURREE": (46, "Murree"),
    "BHAKHAR": (19, "Bhakkar"),
    "KHUSHAB": (17, "Khushab"),
    "MIANWALI": (18, "Mianwali"),
    "SARGODHA": (16, "Sargodha"),
    "KOT ADDU": (43, "Kot Addu"),
    "WAZIRABAD": (44, "Wazirabad")
}


# API URLs and credentials
login_url = os.getenv('DLIMS_LOGIN_URL')
district_app_count_url = os.getenv('DLIMS_DISTRICT_APPCOUNT')
login_username = os.getenv('DLIMS_LOGIN_USER')
login_password = os.getenv('DLIMS_LOGIN_PASSWORD')
auth_username = os.getenv('DLIMS_AUTH_USER')
auth_password = os.getenv('DLIMS_AUTH_PASSWORD')

# login to obtain authentication token
try:
    basic_token = base64.b64encode(f"{auth_username}:{auth_password}".encode()).decode()
    login_payload = {
        "username": login_username,
        "password": login_password
    }
    login_headers = {
        'Authorization': f"Basic {basic_token}",
        "Accept": "application/json"
    }
    login_response = requests.post(login_url, data=login_payload, headers=login_headers)
    login_response.raise_for_status()
    token = login_response.json()["data"]["token"]
    logging.info("Login successful, token obtained.")
except requests.RequestException as e:
    logging.error(f"Login failed due to request error: {e}")
    sys.exit(1)
except KeyError as e:
    logging.error(f"Invalid login response structure: {e}")
    sys.exit(1)

# Headers for API calls
district_headers = {
    "Authorization-Token": f"Bearer {token}",
    "Authorization": f"Basic {basic_token}",
    "Accept": "application/json"
}

# Establish database connection
try:
    conn = get_processed_db_connection()
    cursor = conn.cursor()
    logging.info("Database connection established.")
except Exception as e:
    logging.error(f"Failed to establish database connection: {e}")
    sys.exit(1)

# Create table if it doesn't exist
try:
    create_table_query = """
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
    cursor.execute(create_table_query)
    conn.commit()
    logging.info("Table 'pkm_data' ensured to exist.")
except Exception as e:
    logging.error(f"Failed to create table: {e}")
    conn.rollback()
    cursor.close()
    conn.close()
    sys.exit(1)

# Function to fetch and store data for a given date and district
def fetch_and_store_data(date_str, district_name, district_id):
    payload = {
        "district_id": district_id,
        "start_date": date_str,
        "end_date": date_str
    }

    desired_id, desired_name = district_info[district_name]

    try:
        # Call the DistrictAppCount API
        resp = requests.post(district_app_count_url, data=payload, headers=district_headers)
        resp.raise_for_status()
        body = resp.json()

        # Check if API call was successful
        if not body.get('status'):
            logging.warning(f"API returned error for district {district_id} on {date_str}: {body.get('message')}")
            return

        # Extract the 'response' data
        response_data = body.get('data', {}).get('response', {})
        if not response_data:
            logging.warning(f"Invalid response structure for district {desired_name} on {date_str}")
            return

        # Sum the specified keys for 'complete' (driving licenses issued)
        keys_to_sum = ["NEW_REGULAR", "NEW_INTERNATIONAL", "RENEWAL_OF_REGULAR", "RENEWAL_OF_INTERNATIONAL"]
        total_complete = sum(int(response_data.get(key, '0')) for key in keys_to_sum)

        # Convert date string to date object
        date_obj = datetime.strptime(date_str, '%Y-%m-%d').date()

        # Insert or update data in pkm_data table
        cursor.execute("""
            INSERT INTO pkm_data (district_id, district_name, date, service_name, pending, complete, inprogress)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (district_id, date, service_name)
            DO UPDATE SET
                pending = EXCLUDED.pending,
                complete = EXCLUDED.complete,
                inprogress = EXCLUDED.inprogress;
        """, (desired_id, desired_name, date_obj, 'driving_license_issued', 0, total_complete, 0))
        conn.commit()
        logging.info(f"Data inserted/updated for district {desired_name} on {date_str}")

    except requests.RequestException as e:
        logging.error(f"API request failed for district {desired_name} on {date_str}: {e}")
    except ValueError as e:
        logging.error(f"Error parsing response for district {desired_name} on {date_str}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error for district {desired_name} on {date_str}: {e}")
        conn.rollback()

# Process each day in the date range
for current_date in date_list:
    date_str = current_date.strftime('%Y-%m-%d')
    logging.info(f"Processing data for {date_str}")
    for district_name, district_id in district_mapping.items():
        fetch_and_store_data(date_str, district_name, district_id)
    logging.info("Waiting 5 seconds")
    time.sleep(5)

# Clean up database resources
try:
    cursor.close()
    conn.close()
    logging.info("Database connection closed.")
except Exception as e:
    logging.error(f"Error closing database connection: {e}")

logging.info("DLIMS Data ETL Completed")