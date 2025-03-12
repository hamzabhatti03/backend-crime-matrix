import pandas as pd
import sqlite3
from datetime import datetime

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
    station_clean = station.lower()

    # Remove duplicate designation word from station
    if designation_clean in station_clean:
        station_clean = station_clean.replace(designation_clean, "").strip()

    return f"{designation_clean}.{station_clean}.{short_district}@punjabpolice.gov.pk"


def get_view_role(designation):
    """Assign view role based on designation."""
    designation = designation.lower()
    if "rpo" in designation:
        return 3
    elif "dpo" in designation:
        return 4
    return 5


# Process DataFrame
data = []
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def to_camel_case(text):
    return " ".join(word.capitalize() for word in text.split())


for _, row in df.iterrows():
    username = generate_username(row["Designation"], row["Police station"], row["District"])
    view_role = get_view_role(row["Designation"])

    data.append((
        to_camel_case(row["Designation"]),
        to_camel_case(
            row["Police station"] + " - " + district_short_forms.get(row["District"], row["District"][:3].upper())),
        username,
        "PunjabPolice321",
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
        timestamp,
        row["District"]
    ))

# Store data in SQLite
conn = sqlite3.connect("police_users.db")
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS emergency_users (
    user_id_emergency INTEGER PRIMARY KEY AUTOINCREMENT,
    first_name_emergency TEXT,
    last_name_emergency TEXT,
    user_name_emergency TEXT,
    password_emergency TEXT,
    role_emergency INTEGER,
    jwt_token_emergency TEXT,
    assigned_district_emergency TEXT,
    assigned_division_emergency TEXT,
    assigned_ps_emergency TEXT,
    view_role_emergency INTEGER,
    access_token TEXT,
    last_password_change TEXT,
    status TEXT,
    is_field_officer INTEGER,
    lastseen TEXT,
    district TEXT
)
""")

cursor.executemany("""
INSERT INTO emergency_users (
    first_name_emergency, last_name_emergency, user_name_emergency, password_emergency, role_emergency, 
    jwt_token_emergency, assigned_district_emergency, assigned_division_emergency, 
    assigned_ps_emergency, view_role_emergency, access_token, last_password_change, 
    status, is_field_officer, lastseen, district
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""", data)

conn.commit()
conn.close()

print("Users successfully added to the database.")
