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
    "D. G Khan": "DGK",
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
    "M. B Din": "MBD",
    "Mianwali": "MWL",
    "Multan": "MTN",
    "Muzaffargarh": "MZF",
    "Narowal": "NRW",
    "Nankana": "NSB",
    "Okara": "OKA",
    "Pakpattan": "PKT",
    "Rahimyar Khan": "RYK",
    "Rajanpur": "RJP",
    "Rawalpindi": "RWP",
    "Sahiwal": "SWL",
    "Sargodha": "SGD",
    "Sheikhupura": "SHK",
    "Sialkot": "SKT",
    "T-T Singh": "TTS",
    "Vehari": "VHR"
}

# Read the Excel file
df = pd.read_excel("PunjabLTEUsers.xlsx")


def generate_username(designation, station, district):
    """Generate email username following the given rules."""
    short_district = district_short_forms.get(district, district[:3].upper())
    designation_clean = designation.lower()
    station_clean = station.lower().replace(" ", ".").replace(district.lower(), "").strip(".")

    # Ensure "PUR" stays together
    station_clean = station_clean.replace(".pur.", "pur.").replace(".pur", "pur")

    # Remove any double dots caused by existing dots in station names
    station_clean = station_clean.replace("..", ".")

    return f"{designation_clean}.{station_clean}.{short_district.lower()}@punjabpolice.gov.pk"


def get_view_role(designation):
    """Assign view role based on designation."""
    return 3 if designation.lower() == "rpo" else 4 if designation.lower() == "dpo" else 5


def to_camel_case(text):
    return " ".join(word.upper() for word in text.split())


# Process DataFrame
allowed_designations = {"sho"}  # Define allowed designations
data = []
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

for _, row in df.iterrows():
    if row["Designation"].lower() not in allowed_designations:
        continue

    username = generate_username(row["Designation"], row["Police station"], row["District"])
    view_role = get_view_role(row["Designation"])
    hashed_password = hashlib.md5("PunjabPolice321".encode()).hexdigest()
    station_short = row["Police station"].replace(row["District"], "").strip()
    last_name = f"{station_short} - {district_short_forms.get(row["District"], row["District"][:3].upper())}"

    data.append((
        to_camel_case(row["Designation"]),
        to_camel_case(last_name),
        username,
        hashed_password,
        0,  # role_emergency
        "",  # jwt_token_emergency
        row["District"],
        "",  # assigned_division_emergency
        row["Police station"],
        view_role,
        "",  # access_token
        timestamp,
        "active",
        1,  # is_field_officer
        None,  # lastseen (initially empty)
        row["District"]
    ))

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