import pandas as pd
import psycopg2
import hashlib
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()

# Mapping of district names to short forms
district_short_forms = {
    "Attock": "ATK",
    "Bahawalnagar": "BWN",
    "Bahawalpur": "BWP",
    "Bhakkar": "BKR",
    "Chakwal": "CKL",
    "Chiniot": "CHT",
    "D.G. Khan": "DGK",
    "Faisalabad": "FSD",
    "Gujranwala": "GRW",
    "Gujrat": "GJT",
    "Hafizabad": "HFD",
    "Jhang": "JHG",
    "Jhelum": "JHM",
    "Kasur": "KSR",
    "Khanewal": "KHL",
    "Khushab": "KHB",
    "Lahore": "LHR",
    "Layyah": "LYA",
    "Lodhran": "LDH",
    "M.B. Din": "MBD",
    "Mianwali": "MWL",
    "Multan": "MTN",
    "Muzaffargarh": "MZF",
    "Narowal": "NRW",
    "Nankana Sb": "NSB",
    "Okara": "OKA",
    "Pakpattan": "PKT",
    "Rahimyar Khan": "RYK",
    "Rajanpur": "RJP",
    "Rawalpindi": "RWP",
    "Sahiwal": "SWL",
    "Sargodha": "SGD",
    "Sheikhupura": "SHK",
    "Shiekhupura": "SHK",
    "Sialkot": "SKT",
    "T.T. Singh": "TTS",
    "Vehari": "VHR"
}

# Mapping of district names to match database records
district_db_names = {
    "T.T. Singh": "Toba Tek Singh",
    "D.G. Khan": "D.g Khan",
    "Rahimyar Khan": "Rahim Yar Khan",
    "M.B. Din": "Mandi Baha Ud Din"
}

# Read the Excel file
df = pd.read_excel("psca_15_users_ps_details.xlsx")

# Identify duplicate records based on district, station, and designation
duplicate_records = df[df.duplicated(subset=["District", "Police station", "Designation"], keep=False)]
if not duplicate_records.empty:
    print("Duplicate records found:")
    print(duplicate_records)

# Remove duplicate records based on district, station, and designation
df = df.drop_duplicates(subset=["District", "Police station", "Designation"], keep="first")


def clean_text(text):
    """Trim spaces and standardize dots in text."""
    return text.strip().replace("..", ".")


def to_camel_case(text):
    """Convert text to Camel Case."""
    return " ".join(word.capitalize() for word in text.split())


def fetch_assigned_division(cursor, station, district):
    """Fetch the division name for a given station and district, or assign the circle if division is empty."""
    station = to_camel_case(station.replace("PS.", "").replace("PS", "").replace("SHO", "").strip())
    district = district_db_names.get(district, to_camel_case(district))

    cursor.execute(
        """
        SELECT COALESCE(d.name, c.name) FROM police_stations ps
        LEFT JOIN divisions d ON ps.division_id = d.id
        LEFT JOIN circles c ON ps.circle_id = c.id
        JOIN districts dist ON dist.id = ps.district_id
        WHERE ps.name = %s AND dist.name = %s
        """,
        (station, district)
    )
    result = cursor.fetchone()
    return result[0] if result else ""


def generate_username(designation, station, district):
    """Generate email username following the given rules."""
    short_district = district_short_forms.get(district.strip(), district[:3].upper().replace(".", ""))
    designation_clean = clean_text(designation.lower())
    station_clean = clean_text(
        station.lower().replace("-", "").replace(".", "").replace(" ", ".").replace("(", "").replace(")", "").replace(
            district.lower(), "").replace(".pur.", "pur.").replace(".pur", "pur"))

    # Remove "PS." or "PS " from station name
    station_clean = station_clean.replace("ps.", "").replace("ps", "").strip(".")

    # Ensure no double dotscc
    station_clean = station_clean.replace("..", ".")

    # Remove designation from station if repeated
    if designation_clean in station_clean:
        station_clean = station_clean.replace(designation_clean, "").strip(".")

    return f"{designation_clean}.{station_clean}.{short_district.lower()}@punjabpolice.gov.pk".replace("..", ".")


def get_view_role(designation):
    """Assign view role based on designation."""
    return 3 if designation.lower() == "rpo" else 4 if designation.lower() == "dpo" else 5


def to_upper_case(text):
    return text.upper()


def to_lower_case(text):
    return text.lower()


def format_last_name(station, district, designation):
    """Ensure station names with dots retain correct formatting and remove duplicate designation and district."""
    station_clean = clean_text(station.replace("PS.", "").replace("PS", "")).replace("(", "").replace(")", "")
    if designation.lower() in station_clean.lower():
        station_clean = station_clean.replace(designation, "").strip()
    if district.lower() in station_clean.lower():
        station_clean = station_clean.replace(district, "").strip()
    formatted_station = " ".join(word.upper() if "." in word else word.capitalize() for word in station_clean.split())
    return f"{formatted_station} - {district_short_forms.get(district.strip(), district.replace(". ", "").replace(".", "").upper())}"


# Store data in PostgreSQL
try:
    db_conn = psycopg2.connect(
        dbname=os.getenv('PG_DATABASE'),
        user=os.getenv('PG_USER'),
        password=os.getenv('PG_PASSWORD'),
        host=os.getenv('PG_HOST'),
        port=os.getenv('PG_PORT')
    )
    cursor = db_conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users_by_agent (
        user_id_emergency SERIAL PRIMARY KEY,
        first_name_emergency VARCHAR(255),
        last_name_emergency VARCHAR(255),
        user_name_emergency VARCHAR(255),
        password_emergency VARCHAR(255),
        role_emergency INTEGER,
        jwt_token_emergency VARCHAR(255),
        assigned_district_emergency TEXT,
        assigned_division_emergency TEXT,
        assigned_ps_emergency TEXT,
        view_role_emergency INTEGER,
        access_token TEXT,
        last_password_change VARCHAR,
        status VARCHAR,
        is_field_officer SMALLINT,
        lastseen VARCHAR,
        district VARCHAR(255)
    )
    """)

    data = []
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for _, row in df.iterrows():
        if row["Designation"].strip().lower() not in {"sho"}:  # Define allowed designations
            continue

        username = generate_username(row["Designation"], row["Police station"], row["District"])
        view_role = get_view_role(row["Designation"])
        hashed_password = hashlib.md5("PunjabPolice321".encode()).hexdigest()
        last_name = format_last_name(row["Police station"], row["District"], row["Designation"])
        # assigned_division = fetch_assigned_division(cursor, row["Police station"], row["District"])
        assigned_division = ""

        data.append((
            to_upper_case(row["Designation"].strip()),
            last_name,
            username,
            hashed_password,
            0,  # role_emergency
            "",  # jwt_token_emergency
            row["District"].strip(),
            assigned_division,  # assigned_division_emergency
            row["Police station"].strip(),
            view_role,
            "",  # access_token
            timestamp,
            "active",
            1,  # is_field_officer
            None,  # lastseen (initially empty)
            row["District"].strip()
        ))
    cursor.executemany("""
    INSERT INTO users_by_agent (
        first_name_emergency, last_name_emergency, user_name_emergency, password_emergency, role_emergency, 
        jwt_token_emergency, assigned_district_emergency, assigned_division_emergency, 
        assigned_ps_emergency, view_role_emergency, access_token, last_password_change, 
        status, is_field_officer, lastseen, district
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, data)
    db_conn.commit()
    print("Users successfully added to PostgreSQL database.")

except Exception as e:
    print(f"Error occurred: {e}")

finally:
    if cursor:
        cursor.close()
    if db_conn:
        db_conn.close()
