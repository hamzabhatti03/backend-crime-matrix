from certifi.core import where
from flask import Flask, jsonify, request, abort, g
from flask_caching import Cache
from datetime import datetime, timedelta
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from Utilities import utils, configs, validate
from Utilities import db_config
from math import radians, sin, cos, sqrt, atan2
from flask_jwt_extended import (JWTManager, create_access_token, jwt_required, get_jwt_identity)
import json
import hashlib
import traceback
import os
from dotenv import load_dotenv
from functools import wraps
from predictive_api import yesterday_forecast_db as yesterday_forecast
from werkzeug.utils import secure_filename
from flask import send_from_directory
from predictive_api import forecast_date
from predictive_api import get_category_data
from decimal import Decimal
from psycopg2.extras import RealDictCursor
from psycopg2 import pool as pg_pool
from mysql.connector import pooling as sql_pool
import time
# import firebase_admin
# from firebase_admin import credentials , messaging
# from Services import firebase
import requests
import ast

load_dotenv()

'''
Api code return PUNJAB TODAY Data from CENTRALIZED POSTGRESQL DB
Latest file before this file is api_psg_optimized_v2.py, This file includes the changes i.e.,
1) LOGIN with HRMIS API
'''

# try:
#     cred = credentials.Certificate(os.getenv('FIREBASE_CREDENTIALS'))
#     firebase_admin.initialize_app(cred)
# except Exception as e:
#     print(f"Error initializing Firebase: {e}")

# Initialize connection pools
postgresql_pool = None
mysql_pool = None
notification_pool = None
usersdb_pool = None
log_db_pool = None


def initialize_pools():
    global postgresql_pool, mysql_pool, notification_pool, usersdb_pool, log_db_pool
    try:
        # PostgreSQL connection pool
        postgresql_pool = pg_pool.SimpleConnectionPool(
            minconn=1,  # Minimum number of connections
            maxconn=50,  # Maximum number of connections
            dbname=configs.POSTGRES_PROCESSED_STATS_MAIN['dbname'],
            user=configs.POSTGRES_PROCESSED_STATS_MAIN['user'],
            password=configs.POSTGRES_PROCESSED_STATS_MAIN['password'],
            host=configs.POSTGRES_PROCESSED_STATS_MAIN['host'],
            port=configs.POSTGRES_PROCESSED_STATS_MAIN['port']
        )

        # MySQL connection pool
        mysql_pool = sql_pool.MySQLConnectionPool(
            pool_name="mysql_pool",
            pool_size=12,  # Number of connections in the pool
            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )

        notification_pool = pg_pool.SimpleConnectionPool(
            minconn=1,  # Minimum number of connections
            maxconn=50,  # Maximum number of connections
            dbname=os.getenv('NOTIFICATION_DB'),
            user=configs.POSTGRES_PROCESSED_STATS_MAIN['user'],
            password=configs.POSTGRES_PROCESSED_STATS_MAIN['password'],
            host=configs.POSTGRES_PROCESSED_STATS_MAIN['host'],
            port=configs.POSTGRES_PROCESSED_STATS_MAIN['port']
        )

        usersdb_pool = pg_pool.SimpleConnectionPool(
            minconn=1,  # Minimum number of connections
            maxconn=50,  # Maximum number of connections
            dbname=os.getenv('PG_DATABASE'),
            user=os.getenv('PG_USER'),
            password=os.getenv('PG_PASSWORD'),
            host=os.getenv('PG_HOST'),
            port=os.getenv('PG_PORT')
        )

        log_db_pool = pg_pool.SimpleConnectionPool(
            minconn=1,  # Minimum number of connections
            maxconn=50,  # Maximum number of connections
            dbname=os.getenv('LOGS_DB_NAME'),
            user=os.getenv('PG_USER'),
            password=os.getenv('PG_PASSWORD'),
            host=os.getenv('PG_HOST'),
            port=os.getenv('PG_PORT')
        )

        print("Connection pools initialized successfully.")
    except Exception as e:
        print(f"Error initializing connection pools: {e}")
        raise


def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great-circle distance between two points on the Earth."""
    R = 6371  # Radius of Earth in kilometers

    # Convert degrees to radians
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    return R * c


def filter_lat_longs(lat_longs, max_distance_km=3):
    """Filter out points where the distance to the next point exceeds max_distance_km."""
    if not lat_longs:
        return []  # Return an empty list if input is empty

    filtered = [lat_longs[0]]  # Always include the first point

    for i in range(1, len(lat_longs)):
        prev = filtered[-1]
        curr = lat_longs[i]
        distance = haversine(prev[0], prev[1], curr[0], curr[1])

        if distance <= max_distance_km:
            filtered.append(curr)

    return filtered


app = Flask(__name__)

initialize_pools()

app.config['JWT_SECRET_KEY'] = os.getenv("JWT_SECRET")
app.config['JWT_ACCESS_TOKEN_EXPIRES'] = timedelta(seconds=3600)  # 1 Hour

jwt = JWTManager(app)

cache = Cache(config=configs.CACHE_CONFIGS)
cache.init_app(app)

"""REQUEST RATE LIMITER"""
limiter = Limiter(
    get_remote_address,
    app=app,
    default_limits=configs.DEFAULT_LIMITER
)


def get_log_db_connection():
    log_db_conn = mysql_pool.get_connection()
    log_db_cursor = log_db_conn.cursor()
    return log_db_conn, log_db_cursor


def get_processed_db_connection():
    processed_db_conn = postgresql_pool.getconn()
    processed_db_cursor = processed_db_conn.cursor()
    return processed_db_conn, processed_db_cursor


def get_notification_db_connection():
    if notification_pool is None:
        raise Exception("Notification pool is not initialized. Call `initialize_pools()` before using the database.")

    notification_conn = notification_pool.getconn()
    notification_cursor = notification_conn.cursor()
    return notification_conn, notification_cursor


def get_users_db_connection():
    users_db_conn = usersdb_pool.getconn()
    users_db_cursor = users_db_conn.cursor()
    return users_db_conn, users_db_cursor


def get_log_pg_db_connection():
    log_db_conn = log_db_pool.getconn()
    log_db_cursor = log_db_conn.cursor()
    return log_db_conn, log_db_cursor

@app.after_request
def set_security_headers(response):
    response.headers['Content-Security-Policy'] = "default-src 'self'"
    return response


@app.errorhandler(configs.BAD_REQUEST_ERROR)
def handle_400_error(e):
    return utils.handle_400_error(e)


def require_api_key(func):
    def wrapper(*args, **kwargs):
        api_key = request.headers.get("Crime-Matrix-Key")
        if not api_key or api_key != os.getenv("API_KEY"):
            return jsonify({"message": "Invalid or missing API key"}), 401
        return func(*args, **kwargs)

    wrapper.__name__ = func.__name__
    return wrapper


def require_login_key(func):
    def wrapper(*args, **kwargs):
        api_key = request.headers.get("Login-Access-Key")
        if not api_key or api_key != os.getenv("LOGIN_ACCESS_KEY"):
            return jsonify({"message": "Invalid or missing Login Access key"}), 401
        return func(*args, **kwargs)

    wrapper.__name__ = func.__name__
    return wrapper


@jwt.unauthorized_loader
def unauthorized_response(callback):
    return jsonify({"msg": "Missing Authorization Token"}), 401


@jwt.expired_token_loader
def expired_token_callback(jwt_header, jwt_payload):
    # # Extract request details before JWT validation
    # client_ip = request.remote_addr  # Get client IP
    # user_agent = request.headers.get('User-Agent', 'Unknown')  # Get browser/client details
    # referrer = request.referrer  # Get referrer URL if available
    # request_method = request.method  # HTTP method (GET, POST, etc.)
    # request_url = request.url  # Full request URL
    # request_headers = dict(request.headers)  # Convert headers to dict
    # request_data = request.form.to_dict() if request.form else request.json  # Form or JSON data
    #
    # # Print or log request details
    # print(f"--- Incoming Request ---")
    # print(f"Method: {request_method} URL: {request_url}")
    # print(f"Client IP: {client_ip}")
    # print(f"User-Agent: {user_agent}")
    # print(f"Referrer: {referrer}")
    # print(f"Headers: {request_headers}")
    # print(f"Request Data: {request_data}")
    return jsonify({"msg": "JWT Token has expired"}), 401


def validate_ownership(fn):
    @wraps(fn)
    @jwt_required()
    def wrapper(*args, **kwargs):
        # Extract username from the request
        username = request.form.get('username')

        # Get the current user's identity from the JWT
        current_user = get_jwt_identity()

        # Check if the username matches the JWT identity
        if not username or username != current_user:
            return jsonify({"message": "You do not have permission to access this resource.", "status": False}), 403

        # Proceed with the original function
        return fn(*args, **kwargs)

    return wrapper


"""PUNJAB EMERGENCY-i APIs"""


@app.route(configs.REGISTER['ENDPOINT'], methods=[configs.REGISTER['METHOD']])
@require_api_key
@require_login_key
@limiter.limit(configs.LIMITER)
def register():
    conn,cursor = get_users_db_connection()
    try:
        first_name = request.form.get('first_name')
        last_name = request.form.get('last_name')
        username = request.form.get('user_name')
        password = request.form.get('password')
        assigned_district = request.form.get('district', '')
        assigned_division = request.form.get('division', '')
        assigned_ps = request.form.get('police_station', '')
        current_district = request.form.get('current_district', '')
        view_role = request.form.get('view_role', type=int)
        role_emergency = request.form.get('role_emergency', type=int)

        # Check for missing fields
        if not all([first_name, last_name, username, password, view_role]):
            return jsonify({"error": "All required fields must be provided"}), 400

        # hashed_password = hashlib.md5(password.encode()).hexdigest()

        # Check if username already exists
        cursor.execute("SELECT user_id_emergency FROM users WHERE user_name_emergency = %s", (username,))
        if cursor.fetchone():
            return jsonify({"error": "Username already exists"}), 409

        insert_query = """
            INSERT INTO users 
            (first_name_emergency, last_name_emergency, user_name_emergency, password_emergency, 
            assigned_district_emergency, assigned_division_emergency, assigned_ps_emergency, view_role_emergency, district, role_emergency) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (first_name, last_name, username, password,
                                      assigned_district, assigned_division, assigned_ps, view_role, current_district, role_emergency))

        conn.commit()
        return jsonify({"message": "User registered successfully"}), 201

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        cursor.close()
        usersdb_pool.putconn(conn)


@app.route(configs.LOGIN['ENDPOINT'], methods=[configs.LOGIN['METHOD']])
@require_api_key
@require_login_key
@limiter.limit(configs.LIMITER)
def login():
    users_db_conn, usersdb_cursor = get_users_db_connection()
    db_conn, db_cursor = get_log_pg_db_connection()

    try:
        username = request.form.get('username')
        password = request.form.get('password')
        cnic = request.form.get('cnic')

        if not (username and password and cnic):
            return jsonify({
                'status': False,
                'message': 'Required Username, Password, and CNIC.',
                'data': None
            }), 400

        hashed_pass = hashlib.md5(password.encode()).hexdigest()

        usersdb_cursor.execute("SELECT * FROM users WHERE user_name_emergency = %s", (username,))
        user = usersdb_cursor.fetchone()

        if not user or user[4] != hashed_pass:
            return jsonify({
                'status': False,
                'message': 'Incorrect Username or Password'
            }), 400

        user_status = user[13]

        if user_status.lower() == 'inactive':
            return jsonify({
                'status': False,
                'message': 'Your account is inactive. Please contact the administrator.'
            }), 403

        user_role = user[5]

        # **Skip Validation for Specific Roles**
        if user_role == 1 or username.startswith("dig") or username.startswith("aig") or username.startswith('ig'):
            access_token = create_access_token(identity=username)

            usersdb_cursor.execute("""
                UPDATE users
                SET access_token = %s
                WHERE user_name_emergency = %s
            """, (access_token, username))
            users_db_conn.commit()

            return jsonify({
                'data': {
                    'username': user[3],
                    'id': user[0],
                    'name': f"{user[1]} {user[2]}",
                    'role': user[10],
                    'districts': user[7].decode('utf-8') if isinstance(user[7], bytes) else user[7],
                    'police_stations': user[9].decode('utf-8') if isinstance(user[9], bytes) else user[9]
                },
                'token': access_token,
                'message': "Successfully created Access token",
                'status': True
            }), 200

        # **Proceed with validation for other users**
        officer_data = utils.fetch_officer_data(cnic)

        if not officer_data:
            return jsonify({'status': False, 'message': 'No officer data found for provided CNIC'}), 403

        if 'exception' in officer_data:
            designation_name = officer_data['original']['officer_details'].get("designation_name", "").strip()
            dst_name = officer_data['original']['officer_details'].get("posting_district", "").split(' ')[0].strip().lower() \
                            if officer_data['original']['officer_details'].get("posting_district", "") else None
        else:
            designation_name = officer_data.get("designation_name", "").strip()
            dst_name = officer_data.get("dst_name").split(' ')[0].strip().lower() if officer_data.get('dst_name') else None

        ps_name_eng = officer_data.get("ps_name_eng", "").strip().lower() if officer_data.get("ps_name_eng") else None
        cleaned_ps_name_eng = ps_name_eng.replace("PS. ", "").replace(" ", "").lower() if ps_name_eng else None

        assigned_ps_emergency = user[9].decode('utf-8') if isinstance(user[9], bytes) else user[9]

        if designation_name.startswith("DSP") or designation_name.startswith("SDPO"):
            extracted_ps_name = designation_name.replace("DSP", "").replace("SDPO", "").strip().split(",")[0].lower()

            if not username.startswith("sho."):
                extracted_username_section = username.split("@")[0].split(".")[1].lower()
            else:
                extracted_username_section = None

            if extracted_ps_name != extracted_username_section:
                return jsonify({'status': False, 'message': 'Authentication error: Designation mismatch'}), 403

        elif any(designation in designation_name for designation in ["SSP", "SP"]):
            extracted_district_code = username.split("@")[0].split(".")[-1].lower()
            mapped_district = configs.district_code_mapping.get(extracted_district_code, "").lower()

            if mapped_district != dst_name:
                return jsonify({'status': False, 'message': 'Authentication error: District mismatch'}), 403

        elif any(designation in designation_name for designation in ["DPO", "RPO", "CPO", "CCPO"]):
            extracted_district_code = username.split("@")[0].split(".")[-1].lower()
            mapped_district = configs.district_code_mapping.get(extracted_district_code, "").lower()

            if mapped_district != dst_name:
                return jsonify({'status': False, 'message': 'Authentication error: District mismatch'}), 403

        elif designation_name.lower() == "sho":
            if cleaned_ps_name_eng != assigned_ps_emergency.lower():
                return jsonify({'status': False,
                                'message': 'Authentication error: User is not assigned to this police station'}), 403

        else:
            return jsonify({'status': False, 'message': 'Authentication error: Invalid designation'}), 403

        # Generate access token
        access_token = create_access_token(identity=username)

        # Store access token in the database
        usersdb_cursor.execute("""
            UPDATE users
            SET access_token = %s
            WHERE user_name_emergency = %s
        """, (access_token, username))
        users_db_conn.commit()

        return jsonify({
            'data': {
                'username': user[3],
                'id': user[0],
                'name': f"{user[1]} {user[2]}",
                'role': user[10],
                'districts': user[7].decode('utf-8') if isinstance(user[7], bytes) else user[7],
                'police_stations': user[9].decode('utf-8') if isinstance(user[9], bytes) else user[9]
            },
            'token': access_token,
            'message': "Successfully created Access token",
            'status': True
        }), 200

    except Exception as e:
        utils.log_to_pg_database(db_conn, db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': f'Internal server error, Please Try Again Later.'
        }), 500
    finally:
        db_cursor.close()
        log_db_pool.putconn(db_conn)
        usersdb_cursor.close()
        usersdb_pool.putconn(users_db_conn)


@app.route(configs.UPDATE_PASSWORD['ENDPOINT'], methods=[configs.UPDATE_PASSWORD['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def update_password():
    db_conn, db_cursor = get_log_pg_db_connection()
    users_db_conn, usersdb_cursor = get_users_db_connection()
    try:
        current_user = get_jwt_identity()
        if 'multipart/form-data' not in request.content_type:
            return jsonify({"error": "Invalid request format. Use form-data."}), 400

        current_password = request.form.get('current_password')
        new_password = request.form.get('new_password')
        confirm_password = request.form.get('confirm_password')

        if not all([current_password, new_password]):
            return jsonify({
                'status': False,
                'message': 'All password fields are required.',
            }), 400

        if new_password != confirm_password:
            return jsonify({
                'status': False,
                'message': 'New Password and Confirm Password do not match.',
            }), 400

        if len(new_password) < 8 or len(new_password) > 24:
            return jsonify({
                'status': False,
                'message': 'New password must be between 8 and 24 characters long.',
            }), 400

        if current_password == new_password:
            return jsonify({
                'status': False,
                'message': 'New password cannot be the same as the current password.',
            }), 400

        hashed_current_pass = hashlib.md5(current_password.encode()).hexdigest()
        hashed_new_pass = hashlib.md5(new_password.encode()).hexdigest()

        usersdb_cursor.execute("SELECT * FROM users WHERE user_name_emergency = %s", (current_user,))
        user = usersdb_cursor.fetchone()

        if not user or user[4] != hashed_current_pass:
            return jsonify({
                'status': False,
                'message': 'Current password is incorrect.'
            }), 400

        usersdb_cursor.execute("""
                    UPDATE users
                    SET password_emergency = %s,
                    last_password_change = NOW()
                    WHERE user_name_emergency = %s
                """, (hashed_new_pass, current_user))
        users_db_conn.commit()

        return jsonify({
            'message': "Password updated successfully.",
            'status': True
        }), 200

    except Exception as e:
        error_response = {
            'status': False,
            'message': f'Password update failed, Please try again Later',
        }
        utils.log_to_pg_database(db_conn, db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify(error_response), 500
    finally:
        db_cursor.close()
        log_db_pool.putconn(db_conn)
        # Properly return to the pool without removing it
        usersdb_cursor.close()
        usersdb_pool.putconn(users_db_conn)


@app.route(configs.DASHBOARD_PUNJAB['ENDPOINT'], methods=[configs.DASHBOARD_PUNJAB['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def punjab_stats_dashboard():
    """Get Punjab dashboard statistics.

    Expected POST body:
    {
        "fromDate": "YYYY-MM-DD",  # Optional
        "toDate": "YYYY-MM-DD",   # Optional
        "view_role": int,         # User role ID (2 = All data, 3,4 = District-specific data & 5 = PS specific Data)
        "district":  str          # String of districts
        "police_station" : str    # String of police_stations
    }

    Returns:
        JSON: Dashboard statistics with standardized response format
    """

    predpol_db_conn, predpol_db_cursor = get_log_db_connection()
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    users_db_conn, usersdb_cursor = get_users_db_connection()
    try:
        # Get parameters from request body
        from_date_str = request.form.get('fromDate')
        to_date_str = request.form.get('toDate')
        view_role = request.form.get('view_role', type=int)
        district_str = request.form.get('district')
        police_station_str = request.form.get('police_station')
        username = request.form.get('username')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        # Validate date format
        try:
            datetime.strptime(from_date_str, configs.YM_DATE)
            datetime.strptime(to_date_str, configs.YM_DATE)
        except ValueError:
            return jsonify({
                'status': False,
                'message': 'Invalid date format. Use YYYY-MM-DD',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        if view_role == 5 and district_ids:
            district_condition = (
                f"AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            district_condition = f"AND district_id IN ({', '.join(map(str, district_ids))})"
        else:
            district_condition = ""

        category_query = """
            SELECT 
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Firing on Police', 'Suicidal Attack/ Bomb Blast/ Terrorist Attack')
                ) AS terrorism,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Murder', 'Attempt to Murder')
                ) AS murder,
                COUNT(*) FILTER (
                    WHERE level3_case_nature = 'Aerial Firing'
                ) AS firing,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Sexual Assault/ Harrasment To Women')
                ) AS women_harrasment,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Child Kidnapping', 'Female Kidnapping/ Abduction', 'Male Kidnapping/ Abduction',
                        'Kidnapping for Ransom', 'Attempt to Kidnap / Abduct', 'Child Kidnapping '
                    )
                ) AS kidnapping,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Rape', 'Child Abuse / Molestation')
                ) AS child_abuse,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Prostitution/ Brothel House', 'Acid Throwing', 'Hurt / Injuries', 'Street Fight', 
                        'Other Assault', 'Physical Threats / Harrasment', 'Domestic Violence', 
                        'Criminal Intimidation (Threat with Weapon)'
                    )
                ) AS others_cap,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Highway/Road/Street Dacoity', 'House Dacoity', 'Any Other Dacoity', 
                        'Shop Dacoity', 'Cattle Dacoity', 'Patrol Pump Dacoity', 'Jewellery Shop Dacoity'
                    )
                ) AS dacoity,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Highway/Road/Street Robbery', 'Any Other Robbery', 'Cattle Robbery', 'House Robbery',
                        'Shop Robbery', 'Patrol Pump Robbery', 'Bank/Money Exchange/ ATM Robbery', 'Car Snatching',
                        'Other Vehicles Snatching', 'Snatching/Jhapatta', 'Motorcycle Snatching', 'Jewellery Shop Robbery',
                        'Robbery with Murder'
                    )
                ) AS robbery_snatching,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Mobile Theft', 'Any Other Theft', 'Cattle theft', 'Transformer/ Motor Theft', 'Pick Pocketing',
                        'Purse / Wallet / Luggage Theft', 'Cycle Theft', 'Weapon Theft', 'Other Vehicles Theft',
                        'House Burglary', 'Shop Burglary', 'Other Burglary'
                    )
                ) AS theft,
                COUNT(*) FILTER (
                    WHERE level3_case_nature = 'Motorcycle Theft'
                ) AS motorcycle_theft,
                COUNT(*) FILTER (
                    WHERE level3_case_nature = 'Car Theft'
                ) AS car_theft,
                COUNT(*) FILTER (
                    WHERE level3_case_nature = 'Attempt to Illegal Possession of Land/ Premises'
                ) AS others_property,
                COUNT(*) FILTER (
                    WHERE queue = 'minorities-15'
                ) AS minorities,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('House Burglary', 'Shop Burglary', 'Other Burglary')
                ) AS burglary,
                COUNT(*) FILTER (
                    WHERE level3_case_nature = 'Dacoity with Murder'
                ) AS dacoity_with_murder
            FROM response_time
            WHERE 
                police_station is not Null
                AND response_time IS NOT NULL
                AND date BETWEEN %s AND %s
                AND parent_id=0
                AND response_time > 0
                AND district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
                {district_condition};
        """
        category_query = category_query.format(district_condition=district_condition)
        processed_db_cursor.execute(category_query, [from_date_str, to_date_str])
        category_stats = processed_db_cursor.fetchone()

        calls_cases_query = """
            SELECT 
                SUM(total_calls) AS total_calls,
                SUM(CASE WHEN police_station != 'Unknown' THEN generated_cases ELSE 0 END) AS generated_cases
            FROM 
                processed_data
            WHERE 
                date BETWEEN %s AND %s
                AND district_id NOT IN ('0','41','42','43','44','45','46')
                AND district_id IS NOT NULL
            {district_condition}
        """
        calls_cases_query = calls_cases_query.format(district_condition=district_condition)

        processed_db_cursor.execute(calls_cases_query, [from_date_str, to_date_str])
        total_calls, total_cases = processed_db_cursor.fetchone()

        regional_response_query = """
                    WITH RegionCategories AS (
                SELECT DISTINCT region_category
                FROM response_time
                WHERE region_category IS NOT NULL
            ),
            FilteredData AS (
                SELECT region_category, AVG(response_time) AS avg_response_time
                FROM response_time
                WHERE region_category IS NOT NULL 
                  AND district_id NOT IN ('0','41','42','43','44','45','46') 
                  AND district_id IS NOT NULL 
                  AND parent_id = 0 
                  AND response_time IS NOT NULL 
                  AND response_time > 0 
                  AND date BETWEEN %s AND %s
                    {district_condition}
                GROUP BY region_category
            )
            SELECT rc.region_category, 
                   COALESCE(fd.avg_response_time, 0) AS avg_response_time
            FROM RegionCategories rc
            LEFT JOIN FilteredData fd
            ON rc.region_category = fd.region_category;
        """
        regional_response_query = regional_response_query.format(district_condition=district_condition)
        processed_db_cursor.execute(regional_response_query, [from_date_str, to_date_str])
        regional_avg_responses = processed_db_cursor.fetchall()

        if view_role == 2 or view_role == 1:
            processed_db_cursor.execute(
                utils.build_response_time_query(
                    columns_key="avg_only",
                    additional_conditions=[" date BETWEEN %s AND %s "]
                ),
                [from_date_str, to_date_str]
            )
        else:
            processed_db_cursor.execute(
                utils.build_response_time_query(
                    columns_key="avg_only",
                    additional_conditions=[" date BETWEEN %s AND %s ", f" {district_condition[4:]} "]
                ),
                [from_date_str, to_date_str]
            )

        response_time = processed_db_cursor.fetchall()

        fir_stats_query = """
            SELECT sum(murder), sum(dacoity), sum(aerial_firing), sum(rape), sum(minorities), sum(hurt),
                   sum(burglary), sum(kidnapping), sum(child_abuse),sum(robbery_snatching),
                   sum(motorcycle_theft), sum(dacoity_with_murder), sum(car_theft), sum(theft),
                   sum(terrorist_act), sum(other_person),sum(other_property)
            FROM fir_cases
            WHERE date BETWEEN %s AND %s
            {district_condition}
        """
        fir_stats_query = fir_stats_query.format(district_condition=district_condition)
        processed_db_cursor.execute(fir_stats_query, [from_date_str, to_date_str])
        fir_stats = processed_db_cursor.fetchone()

        fir_count_query = """
            SELECT sum(fir_count) from fir_data where date BETWEEN %s AND %s
            {district_condition}
        """
        fir_count_query = fir_count_query.format(district_condition=district_condition)

        processed_db_cursor.execute(fir_count_query, [from_date_str, to_date_str])
        fir_count = processed_db_cursor.fetchone()[0]

        # Map the data and calculate the dashboard data
        (murder_fir, dacoity_fir, firing_fir, women_harrasment_fir, minorities_fir,
         hurt_fir, burglary_fir, kidnapping_fir, child_abuse_fir, robbery_snatching_fir,
         motorcycle_theft_fir, dacoity_with_murder_fir, car_theft_fir, theft_fir,
         terrorism_fir, other_person_fir, other_property_fir) = fir_stats

        (terrorism, murder, firing, women_harrasment, kidnapping, child_abuse,
         others_person, dacoity, robbery_snatching, theft, motorcycle_theft,
         car_theft, others_property, minorities, burglary, dacoity_with_murder) = category_stats

        category_regional_response_query = """
                    WITH categorized_data AS (
                        SELECT 
                            CASE 
                                WHEN level3_case_nature IN ('Male Kidnapping/ Abduction', 'Female Kidnapping/ Abduction', 
                                                            'Child Kidnapping', 'Attempt to Kidnap / Abduct', 
                                                            'Kidnapping for Ransom', 'Child Kidnapping ') THEN 'Kidnapping'
                                WHEN level3_case_nature IN ('Highway/Road/Street Robbery', 'Any Other Robbery', 'Cattle Robbery',
                                                            'House Robbery', 'Shop Robbery', 'Patrol Pump Robbery', 'Bank/Money Exchange/ ATM Robbery', 'Car Snatching',
                                                            'Other Vehicles Snatching', 'Snatching/Jhapatta', 'Motorcycle Snatching', 'Jewellery Shop Robbery', 'Robbery with Murder') THEN 'robbery_snatching'
                                WHEN level3_case_nature IN ('Mobile Theft', 'Any Other Theft', 'Cattle theft',
                                                            'Transformer/ Motor Theft', 'Pick Pocketing', 'Purse / Wallet / Luggage Theft',
                                                            'Cycle Theft', 'Weapon Theft', 'Other Burglary', 'Shop Burglary', 'House Burglary') THEN 'theft'
                                WHEN queue = 'minorities-15' THEN 'minorities'
                                WHEN level3_case_nature IN ('Dacoity with Murder') THEN 'dacoity_with_murder'
                                WHEN level3_case_nature IN ('Highway/Road/Street Dacoity', 'House Dacoity', 'Any Other Dacoity', 'Shop Dacoity', 'Cattle Dacoity', 'Patrol Pump Dacoity', 'Jewellery Shop Dacoity') THEN 'dacoity'
                                WHEN level3_case_nature IN ('Sexual Assault/ Harrasment To Women') THEN 'rape'
                                WHEN level3_case_nature IN ('Firing on Police', 'Suicidal Attack/ Bomb Blast/ Terrorist Attack') THEN 'terrorist_act'
                                WHEN level3_case_nature IN ('Rape', 'Child Abuse / Molestation') THEN 'child_abuse'
                                WHEN level3_case_nature IN ('Murder', 'Attempt to Murder') THEN 'murder'
                                WHEN level3_case_nature IN ('Car Theft') THEN 'car_theft'
                                WHEN level3_case_nature = 'Aerial Firing' THEN 'aerial_firing'
                                WHEN level3_case_nature = 'Motorcycle Theft' THEN 'motorcycle_theft'
                                WHEN level3_case_nature = 'Attempt to Illegal Possession of Land/ Premises' THEN 'other_property'
                                WHEN level3_case_nature IN ('Prostitution/ Brothel House', 'Acid Throwing', 'Hurt / Injuries', 'Street Fight', 'Other Assault',
                                                            'Physical Threats / Harrasment', 'Domestic Violence', 'Criminal Intimidation (Threat with Weapon)') THEN 'other_person'
                                WHEN level3_case_nature IN ('Assault on Govt. Officials', 'Other Help') THEN 'hurt'
                            END AS category,
                            region_category,
                            AVG(response_time) AS avg_response_time
                        FROM 
                            response_time
                        WHERE 
                            level3_case_nature IN (
                                'Murder', 'Robbery with Murder', 'Dacoity with Murder', 'Attempt to Murder', 'Aerial Firing', 'Child Abuse / Molestation', 'Motorcycle Theft',
                                'Male Kidnapping/ Abduction', 'Female Kidnapping/ Abduction', 'Child Kidnapping', 'Attempt to Kidnap / Abduct', 'Kidnapping for Ransom', 'Child Kidnapping ',
                                'Rape', 'Sexual Assault/ Harrasment To Women', 'Highway/Road/Street Robbery', 'Any Other Robbery', 'Cattle Robbery', 'House Robbery', 'Shop Robbery',
                                'Patrol Pump Robbery', 'Bank/Money Exchange/ ATM Robbery', 'Car Snatching', 'Other Vehicles Snatching', 'Snatching/Jhapatta', 'Motorcycle Snatching', 'Jewellery Shop Robbery',
                                'Highway/Road/Street Dacoity', 'House Dacoity', 'Any Other Dacoity', 'Shop Dacoity', 'Cattle Dacoity', 'Patrol Pump Dacoity', 'Jewellery Shop Dacoity', 'Car Theft',
                                'Mobile Theft', 'Any Other Theft', 'House Burglary', 'Shop Burglary', 'Other Burglary', 'Cattle theft', 'Transformer/ Motor Theft', 'Pick Pocketing', 'Purse / Wallet / Luggage Theft', 'Cycle Theft', 'Weapon Theft',
                                'Firing on Police', 'Suicidal Attack/ Bomb Blast/ Terrorist Attack', 'Hurt / Injuries', 'Street Fight', 'Other Assault', 'Criminal Intimidation (Threat with Weapon)',
                                'Assault on Govt. Officials', 'Acid Throwing', 'Other Help', 'Prostitution/ Brothel House', 'Acid Throwing', 'Hurt / Injuries', 'Street Fight', 'Other Assault',
                                'Physical Threats / Harrasment', 'Domestic Violence', 'Criminal Intimidation (Threat with Weapon)', 'Attempt to Illegal Possession of Land/ Premises'
                            )
                            AND date BETWEEN %s AND %s
                            AND parent_id = 0
                            AND response_time IS NOT NULL 
                            AND response_time > 0
                            {district_condition}
                            AND region_category IS NOT NULL
                        GROUP BY 
                            category, region_category
                    )
                    SELECT *
                    FROM categorized_data
                    WHERE category IS NOT NULL;
        """

        category_regional_response_query = category_regional_response_query.format(
            district_condition=district_condition)

        processed_db_cursor.execute(category_regional_response_query, (from_date_str, to_date_str))
        category_regional_response_times = processed_db_cursor.fetchall()

        category_regional_response_dict = {}
        for category, region, avg_time in category_regional_response_times:
            if category not in category_regional_response_dict:
                category_regional_response_dict[category] = {}
            category_regional_response_dict[category][region] = f"{int(avg_time // 60)}:{int(avg_time % 60):02d}"

        categorical_avg_responsetime_query = """        
                    WITH categorized_data AS (
                        SELECT
                            CASE 
                                WHEN level3_case_nature IN ('Male Kidnapping/ Abduction', 'Female Kidnapping/ Abduction', 
                                                            'Child Kidnapping', 'Attempt to Kidnap / Abduct', 
                                                            'Kidnapping for Ransom', 'Child Kidnapping ') 
                                THEN 'Kidnapping'
                                WHEN level3_case_nature IN ('Highway/Road/Street Robbery','Any Other Robbery', 'Cattle Robbery',
                                                            'House Robbery', 'Shop Robbery','Patrol Pump Robbery','Bank/Money Exchange/ ATM Robbery','Car Snatching',
                                                            'Other Vehicles Snatching','Snatching/Jhapatta','Motorcycle Snatching','Jewellery Shop Robbery','Robbery with Murder')
                                THEN 'robbery_snatching'
                                WHEN level3_case_nature IN ('Mobile Theft','Any Other Theft','Cattle theft',
                                                            'Transformer/ Motor Theft','Pick Pocketing','Purse / Wallet / Luggage Theft',
                                                            'Cycle Theft','Weapon Theft','Other Burglary','Shop Burglary','House Burglary')
                                THEN 'theft'
                                WHEN queue = 'minorities-15' THEN 'minorities'
                                WHEN level3_case_nature IN ('Dacoity with Murder') THEN 'dacoity_with_murder'
                                WHEN level3_case_nature IN ('Highway/Road/Street Dacoity','House Dacoity','Any Other Dacoity', 'Shop Dacoity', 'Cattle Dacoity','Patrol Pump Dacoity','Jewellery Shop Dacoity') THEN 'dacoity'
                                WHEN level3_case_nature IN ('Sexual Assault/ Harrasment To Women') THEN 'rape'
                                WHEN level3_case_nature IN ('Firing on Police','Suicidal Attack/ Bomb Blast/ Terrorist Attack') THEN 'terrorist_act'
                                WHEN level3_case_nature IN ('Rape','Child Abuse / Molestation') THEN 'child_abuse'
                                WHEN level3_case_nature IN ('Murder','Attempt to Murder') THEN 'murder'
                                WHEN level3_case_nature IN ('Car Theft') THEN 'car_theft'
                                WHEN level3_case_nature = 'Aerial Firing' THEN 'aerial_firing'
                                WHEN level3_case_nature = 'Motorcycle Theft' THEN 'motorcycle_theft'
                                WHEN level3_case_nature = 'Attempt to Illegal Possession of Land/ Premises' THEN 'other_property'
                                WHEN level3_case_nature IN ('Prostitution/ Brothel House','Acid Throwing','Hurt / Injuries','Street Fight','Other Assault',
                                  'Physical Threats / Harrasment','Domestic Violence','Criminal Intimidation (Threat with Weapon)') THEN 'other_person'
                                WHEN level3_case_nature IN ('Assault on Govt. Officials','Other Help') THEN 'hurt'
                            END AS category,
                            response_time
                        FROM 
                            response_time
                        WHERE 
                            level3_case_nature IN (
                                'Murder','Robbery with Murder','Dacoity with Murder', 'Attempt to Murder', 'Aerial Firing','Child Abuse / Molestation', 'Motorcycle Theft',
                                'Male Kidnapping/ Abduction', 'Female Kidnapping/ Abduction', 'Child Kidnapping', 'Attempt to Kidnap / Abduct', 'Kidnapping for Ransom', 'Child Kidnapping ',
                                'Rape','Sexual Assault/ Harrasment To Women','Highway/Road/Street Robbery','Any Other Robbery', 'Cattle Robbery','House Robbery', 'Shop Robbery',
                                'Patrol Pump Robbery','Bank/Money Exchange/ ATM Robbery','Car Snatching','Other Vehicles Snatching','Snatching/Jhapatta','Motorcycle Snatching','Jewellery Shop Robbery',
                                'Highway/Road/Street Dacoity','House Dacoity' ,'Any Other Dacoity' , 'Shop Dacoity' ,'Cattle Dacoity','Patrol Pump Dacoity','Jewellery Shop Dacoity','Car Theft',
                                'Mobile Theft','Any Other Theft','House Burglary','Shop Burglary','Other Burglary','Cattle theft','Transformer/ Motor Theft','Pick Pocketing','Purse / Wallet / Luggage Theft','Cycle Theft','Weapon Theft',
                                'Firing on Police','Suicidal Attack/ Bomb Blast/ Terrorist Attack','Hurt / Injuries','Street Fight','Other Assault','Criminal Intimidation (Threat with Weapon)',
                                'Assault on Govt. Officials','Acid Throwing','Other Help','Prostitution/ Brothel House','Acid Throwing','Hurt / Injuries','Street Fight','Other Assault',
                                'Physical Threats / Harrasment','Domestic Violence','Criminal Intimidation (Threat with Weapon)','Attempt to Illegal Possession of Land/ Premises'
                            )
                            AND date BETWEEN %s AND %s
                            AND parent_id = 0
                            AND response_time IS NOT NULL 
                            AND response_time > 0
                            {district_condition}
                    )
                    SELECT 
                        category,
                        AVG(response_time) AS avg_response_time
                    FROM 
                        categorized_data
                    WHERE 
                        category IS NOT NULL
                    GROUP BY 
                        category;"""

        categorical_avg_responsetime_query = categorical_avg_responsetime_query.format(
            district_condition=district_condition)

        processed_db_cursor.execute(categorical_avg_responsetime_query, (from_date_str, to_date_str))
        category_avg_response_times = processed_db_cursor.fetchall()
        category_avg_response_dict = {}
        for category, avg_time in category_avg_response_times:
            if category not in category_avg_response_dict:
                category_avg_response_dict[category] = {}
            category_avg_response_dict[category] = f"{int(avg_time // 60)}:{int(avg_time % 60):02d}"

        where_cond = ""
        if view_role in [3, 4]:
            where_cond = f"AND district_id IN ({', '.join(map(str, district_ids))})"

        ############################### Negative caller Feedback
        negative_caller_feedback_query = """
                                    Select SUM(CASE WHEN caller_feedback = 'Negative' THEN 1 ELSE 0 END)
                                    FROM response_time
                                    WHERE date Between %s AND %s 
                                    {where_cond}
                                """
        negative_caller_feedback_query = negative_caller_feedback_query.format(where_cond=where_cond)
        processed_db_cursor.execute(negative_caller_feedback_query, (from_date_str, to_date_str))
        row = processed_db_cursor.fetchone()
        negative_feedback_count = row[0] if row is not None else 0

        ############################## Escalated Cases Count (VWPS , VCCS , VCM)

        from_date_obj = datetime.strptime(from_date_str, '%Y-%m-%d')
        to_date_obj = datetime.strptime(to_date_str, '%Y-%m-%d')

        current_date_str = from_date_obj.strftime('%Y-%m-%d 00:00:00')
        to_date_str = to_date_obj.strftime('%Y-%m-%d 23:59:59')

        if view_role == 5 and district_ids:
            additional_cond = (
                f"AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND pucar_police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            additional_cond = f"AND district_id IN ({', '.join(map(str, district_ids))})"
        else:
            additional_cond = ""

        vwps_conn = db_config.get_vwps_db_connection()
        vwps_cursor = vwps_conn.cursor()

        vccs_conn = db_config.get_vccs_db_connection()
        vccs_cursor = vccs_conn.cursor()

        vcm_conn = db_config.get_vcm_db_connection()
        vcm_cursor = vcm_conn.cursor()

        vwps_escalated_query = f"""
                            SELECT count(*)
                            FROM case_final_status
                            WHERE final_status_id = 7
                            AND district_id is not Null
                            AND created_at BETWEEN %s AND %s
                            {additional_cond}
        """
        vwps_cursor.execute(vwps_escalated_query, (current_date_str, to_date_str))
        row = vwps_cursor.fetchone()
        vwps_escalated = row[0] if row is not None else 0

        vccs_escalated_query = f"""
                                    SELECT count(*)
                                    FROM case_final_status
                                    WHERE final_status_id = 7
                                    AND district_id is not Null
                                    AND created_at BETWEEN %s AND %s
                                    {additional_cond}
                """
        vccs_cursor.execute(vccs_escalated_query, (current_date_str, to_date_str))
        row = vccs_cursor.fetchone()
        vccs_escalated = row[0] if row is not None else 0

        vcm_escalated_query = f"""
                                    SELECT count(*)
                                    FROM case_final_status
                                    WHERE final_status_id = 7
                                    AND district_id is not Null
                                    AND created_at BETWEEN %s AND %s
                                    {additional_cond}
                """
        vcm_cursor.execute(vcm_escalated_query, (current_date_str, to_date_str))
        row = vcm_cursor.fetchone()
        vcm_escalated = row[0] if row is not None else 0

        escalated_case_count = vwps_escalated + vccs_escalated + vcm_escalated

        alerts_count_query = """
                            SELECT district_id, COUNT(case_number) AS case_count
                            FROM response_time
                            Where date between %s AND %s
                            AND response_time > 2100 
                            AND parent_id = 0
                            AND district_id IS NOT NULL
                            AND district_id NOT IN ('0','41','42','43','44','45','46')
                            AND level2_case_nature in ('Robbery/Snatching', 'Burglary', 'Dacoity', 'Sexual Assault', 
                            'Kiddnapping / Abduction', 'Murder', 'Terrorist Act')
                            {district_condition}
                            GROUP BY district_id
                """
        alerts_count_query = alerts_count_query.format(district_condition=district_condition)

        processed_db_cursor.execute(alerts_count_query, (from_date_str, to_date_str))
        alerts = processed_db_cursor.fetchall()

        total_alerts = 0
        for i in alerts:
            try:
                total_alerts += int(i[1])
            except (ValueError, IndexError) as e:
                print(f"Error processing alert {i}: {e}")

        response_time_alerts = {"response_time_alerts": total_alerts}

        if view_role == 5 and district_ids:
            additional_condition = (
                f"  AND district IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            additional_condition = f" AND district IN ({', '.join(map(str, district_ids))})"
        else:
            additional_condition = ""

        current_date = datetime.now()

        predpol_db_cursor.execute(f"""
                    SELECT count(*)
                    FROM pred_pol_crimes_hotspot 
                    WHERE case_number IS NOT null
                    AND case_nature in ('Highway/Road/Street Robbery', 'Shop Robbery', 'Patrol Pump Robbery', 'Any Other Robbery', 
                    'Snatching/Jhapatta', 'House Robbery', 'Cattle Robbery', 'Robbery with Murder', 'Bank/Money Exchange/ ATM Robbery', 
                    'Jewellery Shop Robbery','Motorcycle Snatching', 'Bank Burglary', 'House Burglary', 'Shop Burglary', 'Other Burglary', 
                    'Dacoity with Murder', 'House Dacoity', 'Highway/Road/Street Dacoity', 'Cattle Dacoity', 'Shop Dacoity', 'Any Other Dacoity', 
                    'Patrol Pump Dacoity', 'Jewellery Shop Dacoity', 'Sexual Assault/ Harrasment To Women', 'Rape', 'Child Abuse / Molestation', 
                    'Male Kidnapping/ Abduction', 'Female Kidnapping/ Abduction', 'Attempt to Kidnap / Abduct', 'Child Kidnapping', 
                    'Kidnapping for Ransom','Illegal detention of a person', 'Attempt to Murder', 'Murder', 'Firing on Police', 
                    'Suicidal Attack/ Bomb Blast/ Terrorist Attack')
                    AND date = %s
                    {additional_condition}
        """, (current_date.strftime('%d-%m-%Y'),))
        re_occurrences_count = predpol_db_cursor.fetchone()
        if re_occurrences_count is not None:
            re_occurrences_response = {"reoccured_cases": re_occurrences_count[0]}
        else:
            re_occurrences_response = {"reoccured_cases": 0}

        dashboard_data = {
            'terrorist_act': {'count': terrorism, 'fir': terrorism_fir,
                              'fake/other': 0,
                              'response_times': category_regional_response_dict.get('terrorist_act',
                                                                                    {'urban': '00:00',
                                                                                     'rural': '00:00'}),
                              'avg_response_time': category_avg_response_dict.get('terrorist_act',
                                                                                  '00:00')},
            'murder': {'count': murder, 'fir': murder_fir,
                       'fake/other': 0,
                       'response_times': category_regional_response_dict.get('murder',
                                                                             {'urban': '00:00', 'rural': '00:00'}),
                       'avg_response_time': category_avg_response_dict.get('murder',
                                                                           '00:00')
                       },
            'firing': {'count': firing, 'fir': firing_fir,
                       'fake/other': 0,
                       'response_times': category_regional_response_dict.get('aerial_firing',
                                                                             {'urban': '00:00', 'rural': '00:00'}),
                       'avg_response_time': category_avg_response_dict.get('aerial_firing',
                                                                           '00:00')},
            'kidnapping': {'count': kidnapping, 'fir': kidnapping_fir,
                           'fake/other': 0,
                           'response_times': category_regional_response_dict.get('Kidnapping',
                                                                                 {'urban': '00:00', 'rural': '00:00'}),
                           'avg_response_time': category_avg_response_dict.get('Kidnapping',
                                                                               '00:00')},
            'other_person': {'count': others_person, 'fir': other_person_fir,
                             'fake/other': 0,
                             'response_times': category_regional_response_dict.get('other_person',
                                                                                   {'urban': '00:00',
                                                                                    'rural': '00:00'}),
                             'avg_response_time': category_avg_response_dict.get('other_person',
                                                                                 '00:00')},
            'dacoity': {'count': dacoity, 'fir': dacoity_fir,
                        'fake/other': 0,
                        'response_times': category_regional_response_dict.get('dacoity',
                                                                              {'urban': '00:00', 'rural': '00:00'}),
                        'avg_response_time': category_avg_response_dict.get('dacoity',
                                                                            '00:00')},
            'dacoity_with_murder': {'count': dacoity_with_murder, 'fir': dacoity_with_murder_fir,
                                    'fake/other': 0,
                                    'response_times': category_regional_response_dict.get('dacoity_with_murder',
                                                                                          {'urban': '00:00',
                                                                                           'rural': '00:00'}),
                                    'avg_response_time': category_avg_response_dict.get('dacoity_with_murder',
                                                                                        '00:00')},
            'robbery_snatching': {'count': robbery_snatching, 'fir': robbery_snatching_fir,
                                  'fake/other': 0,
                                  'response_times': category_regional_response_dict.get('robbery_snatching',
                                                                                        {'urban': '00:00',
                                                                                         'rural': '00:00'}),
                                  'avg_response_time': category_avg_response_dict.get('robbery_snatching',
                                                                                      '00:00')},
            'theft': {'count': theft, 'fir': theft_fir,
                      'fake/other': 0,
                      'response_times': category_regional_response_dict.get('theft',
                                                                            {'urban': '00:00', 'rural': '00:00'}),
                      'avg_response_time': category_avg_response_dict.get('theft',
                                                                          '00:00')},
            'child_abuse': {'count': child_abuse, 'fir': child_abuse_fir,
                            'fake/other': 0,
                            'response_times': category_regional_response_dict.get('child_abuse',
                                                                                  {'urban': '00:00', 'rural': '00:00'}),
                            'avg_response_time': category_avg_response_dict.get('child_abuse',
                                                                                '00:00')},
            'motorcycle_theft': {'count': motorcycle_theft, 'fir': motorcycle_theft_fir,
                                 'fake/other': 0,
                                 'response_times': category_regional_response_dict.get('motorcycle_theft',
                                                                                       {'urban': '00:00',
                                                                                        'rural': '00:00'}),
                                 'avg_response_time': category_avg_response_dict.get('motorcycle_theft',
                                                                                     '00:00')},
            'car_theft': {'count': car_theft, 'fir': car_theft_fir,
                          'fake/other': 0,
                          'response_times': category_regional_response_dict.get('car_theft',
                                                                                {'urban': '00:00', 'rural': '00:00'}),
                          'avg_response_time': category_avg_response_dict.get('car_theft',
                                                                              '00:00')},
            'minorities': {'count': minorities, 'fir': minorities_fir,
                           'fake/other': 0,
                           'response_times': category_regional_response_dict.get('minorities',
                                                                                 {'urban': '00:00', 'rural': '00:00'}),
                           'avg_response_time': category_avg_response_dict.get('minorities',
                                                                               '00:00')},
            'burglary': {'count': burglary, 'fir': burglary_fir,
                         'fake/other': 0,
                         'response_times': category_regional_response_dict.get('burglary',
                                                                               {'urban': '00:00', 'rural': '00:00'}),
                         'avg_response_time': category_avg_response_dict.get('burglary',
                                                                             '00:00')},
            'other_property': {'count': others_property, 'fir': other_property_fir,
                               'fake/other': 0,
                               'response_times': category_regional_response_dict.get('other_property',
                                                                                     {'urban': '00:00',
                                                                                      'rural': '00:00'}),
                               'avg_response_time': category_avg_response_dict.get('other_property',
                                                                                   '00:00')},
            'fir_count': fir_count,
            'total_calls': total_calls,
            'total_cases': total_cases,
            'avg_response_time': f"{int(response_time[0][0] // 60)}:{int(response_time[0][0] % 60):02d}" if response_time else 0,
            'rural_response_time': f"{int(regional_avg_responses[0][1] // 60)}:{int(regional_avg_responses[0][1] % 60):02d}" if regional_avg_responses else 0,
            'urban_response_time': f"{int(regional_avg_responses[1][1] // 60)}:{int(regional_avg_responses[1][1] % 60):02d}" if regional_avg_responses else 0,
            'police_encounter': 0,
            'environment_smog': 0,
            'negative_feedbacks': 0 if view_role == 5 else negative_feedback_count,
            'escalated_cases': escalated_case_count,
            'vwps_escalated': vwps_escalated,
            'vcm_escalated': vcm_escalated,
            'vccs_escalated': vccs_escalated,
            'responsetime_alerts': response_time_alerts,
            'crime_reoccurrence_count': re_occurrences_response
        }

        # UPDATE operation for user lastseen logs
        user_update_query = """
                UPDATE users 
                SET lastseen = %s
                WHERE user_name_emergency = %s;
                        """
        usersdb_cursor.execute(user_update_query, (datetime.now(), username))
        users_db_conn.commit()

        response = {
            'status': True,
            'message': 'Dashboard data fetched successfully',
            'data': dashboard_data
        }

        return jsonify(response), 200

    except ValueError as ve:
        return jsonify({
            'status': False,
            'message': str(ve),
            'data': None
        }), 400

    except Exception as e:
        error_response = {
            'status': False,
            'message': f'Internal server error : {e}',
            'data': None
        }
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify(error_response), 500
    finally:
        if predpol_db_cursor:
            predpol_db_cursor.close()
        if predpol_db_conn:
            predpol_db_conn.close()

        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it

        usersdb_cursor.close()
        usersdb_pool.putconn(users_db_conn)

        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.PUNJAB_MORE_INFO['ENDPOINT'], methods=[configs.PUNJAB_MORE_INFO['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def punjab_more_info():
    """Get detailed Punjab information for a specific category.

    Expected POST body:
    {
        "fromDate": "YYYY-MM-DD",  # Optional
        "toDate": "YYYY-MM-DD",   # Optional
        "category" : str          # Case Nature
        "view_role": int,         # User role ID (2 = All data, 3,4 = District-specific data & 5 = PS specific Data)
        "district":  str          # String of districts
        "police_station" : str    # String of police_stations
    }

    Returns:
        JSON: Detailed statistics with standardized response format
    """
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()

    try:
        # Get and validate request body
        # Use request.form to get parameters from form body

        category = request.form.get('category')
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        view_role = request.form.get('view_role', type=int)
        district_str = request.form.get('district')
        police_station_str = request.form.get('police_station')

        if not all([category, from_date, to_date, view_role]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        # Handle category mappings
        category = "rape" if category == "women_harrassment" else category
        category = "aerial_firing" if category == "firing" else category

        # Handle special categories
        if category in ['police_encounter', 'environment_smog_issues']:
            return jsonify({
                'status': True,
                'message': 'No data available for this category',
                'data': {
                    'district_wise': {},
                    'ps_wise': {},
                    'cases': [],
                    'ps_count': {}
                }
            }), 200

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        if view_role not in [1, 2, 3, 4, 5]:
            return jsonify({
                'status': False,
                'message': 'Invalid view role. Use 2 or 3.',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        district_condition = ""
        if (view_role == 3 or view_role == 4) and district_ids:
            district_condition = f"AND district_id IN ({', '.join(map(str, district_ids))})"
        elif view_role == 5 and district_ids:
            district_condition = (
                f"AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )

        district_query = """
                    WITH categorized_data AS (
                        SELECT
                            district_id,
                            CASE
                                WHEN {where_condition}
                                THEN '{category}'
                                ELSE NULL
                            END AS category
                        FROM response_time
                        WHERE date BETWEEN %s AND %s
                          AND parent_id = 0
                          AND response_time IS NOT NULL
                          AND response_time > 0
                          AND police_station IS NOT NULL
                          AND district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
                          {district_condition}
                    )
                    SELECT
                        district_id,
                        COUNT(*) AS count
                    FROM categorized_data
                    WHERE category = '{category}'
                    GROUP BY district_id
                    HAVING COUNT(*) > 0;
                """

        # Handling 'minorities' category separately
        if category == 'minorities':
            where_condition = "queue = 'minorities-15'"
            query_params = [from_date, to_date]
        else:
            level3_values = configs.CATEGORIES.get(category, [])
            level3_list = ", ".join(f"'{val}'" for val in level3_values)
            where_condition = f"level3_case_nature IN ({level3_list})"
            query_params = [from_date, to_date]

        # Formatting the query with appropriate conditions
        district_query = district_query.format(
            where_condition=where_condition,
            category=category,
            district_condition=district_condition
        )

        # Executing the query
        processed_db_cursor.execute(district_query, query_params)
        district_response = processed_db_cursor.fetchall()

        district_response_obj = {
            configs.DISTRICTS_DICTIONARY[int(district_id)]: count
            for district_id, count in district_response
        }

        if category == 'minorities':
            where_condition = "queue = 'minorities-15'"
            query_params = [from_date, to_date]
        else:
            categories = configs.CATEGORIES.get(category)
            where_condition = f"level3_case_nature IN ({','.join(['%s'] * len(categories))})"
            query_params = categories + [from_date, to_date] + categories

        ps_query = """
            SELECT
                district_id,
                police_station_id,
                police_station,
                COUNT(
                    CASE 
                        WHEN {where_condition} THEN 'cc'
                    END
                ) AS count
            FROM 
                response_time
            WHERE 
                date BETWEEN %s AND %s
                    AND parent_id = 0
                    AND response_time IS NOT NULL
                    AND police_station is not Null
                    AND response_time > 0
                    {district_condition}
                    AND district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
            GROUP BY 
                police_station_id, district_id, police_station
            HAVING 
                COUNT(
                    CASE 
                        WHEN {where_condition} THEN 'cc'
                    END
                ) > 0;
        """
        ps_query = ps_query.format(where_condition=where_condition, district_condition=district_condition)

        processed_db_cursor.execute(ps_query, query_params)
        ps_response = processed_db_cursor.fetchall()

        ps_response_obj = {
            ps: {
                configs.DISTRICTS_DICTIONARY.get(district): count}
            for district, ps_id, ps, count in ps_response
        }

        ps_conn = utils.get_db_connection(configs.POLICE_STATIONS_MAIN)
        ps_cursor = ps_conn.cursor()

        ps_condition = ""
        if view_role == 3 or view_role == 4 or view_role == 5:
            ps_condition = f"AND district_id IN ({', '.join(map(str, district_ids))})"

        ps_cursor.execute(f"""
                    SELECT district_id, count(name) as count 
                    FROM police_stations 
                    WHERE district_id NOT IN ('0', '41', '42', '43', '44', '45', '46') 
                        AND district_id is NOT NULL
                        {ps_condition}
                    GROUP BY district_id
                    HAVING count > 1
                """)
        ps_count = ps_cursor.fetchall()
        ps_count_obj = {
            configs.DISTRICTS_DICTIONARY.get(int(district_id)): count
            for district_id, count in ps_count
        }

        # Get case details
        base_query = """
                    SELECT case_number, level3_case_nature, caller_name, caller_number, 
                           accepted_time, police_station, district_id, time_id, 
                           description, first_arrival_time, response_time
                    FROM response_time
                    WHERE {condition}
                        AND district_id NOT IN ('0','41','42','43','44','45','46')
                        AND police_station is not Null
                        AND response_time IS NOT NULL
                        AND date BETWEEN %s AND %s
                        AND parent_id=0
                        AND response_time > 0
                        {district_condition}

                """
        if category == 'minorities':
            where_condition = "queue = 'minorities-15'"
            query_params = [from_date, to_date]
        else:
            categories = configs.CATEGORIES.get(category)
            where_condition = f"level3_case_nature IN ({','.join(['%s'] * len(categories))})"
            query_params = categories + [from_date, to_date]

        cases_query = base_query.format(condition=where_condition, district_condition=district_condition)
        processed_db_cursor.execute(cases_query, query_params)
        cases = processed_db_cursor.fetchall()

        cases_list = [
            {
                "case_number": case_number,
                "case_nature": level3_case_nature,
                "caller_name": caller_name,
                "assigned_time": created_time,
                "cli": caller_number,
                "police_station": police_station,
                "status": 'Completed',
                "district": configs.DISTRICTS_DICTIONARY.get(district_id),
                "time_id": datetime.fromtimestamp(int(time_id)).strftime(configs.YMD_HMS),
                "description": description,
                "reached_time": datetime.fromtimestamp(int(reached_time)).strftime(
                    configs.YMD_HMS) if reached_time else None,
                "response_time": f"{int(response_time // 60)}:{int(response_time % 60):02d}" if response_time else 0
            }
            for case_number, level3_case_nature, caller_name, caller_number, created_time,
            police_station, district_id, time_id, description, reached_time, response_time in cases
        ]

        response = {
            'status': True,
            'message': 'Data fetched successfully',
            'data': {
                'district_wise': district_response_obj,
                'ps_wise': ps_response_obj,
                'cases': cases_list,
                'ps_count': ps_count_obj
            }
        }
        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': 'Internal server error',
            'data': None
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)

        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.DISTRICTWISE_STATS['ENDPOINT'], methods=[configs.DISTRICTWISE_STATS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def districtwise_counts():
    """Get district-wise crime statistics.

    Expected POST body:
    {
        "fromDate": "YYYY-MM-DD",  # Optional
        "toDate": "YYYY-MM-DD",   # Optional
        "view_role": int,         # User role ID (2 = All data, 3,4 = District-specific data & 5 = PS specific Data)
        "district":  str          # String of districts
        "police_station" : str    # String of police_stations
    }

    Returns:
        JSON: District-wise case counts with standardized response format
    """
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()

    try:
        from_date = request.form.get('fromDate', datetime.now().strftime(configs.YM_DATE))
        to_date = request.form.get('toDate', datetime.now().strftime(configs.YM_DATE))
        view_role = request.form.get('view_role', type=int)
        district_str = request.form.get('district')
        police_station_str = request.form.get('police_station')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        if not all([from_date, to_date, view_role]):
            return jsonify({
                'status': False,
                'message': 'Missing required date parameters',
                'data': None
            }), 400

        # Validate date format
        try:
            datetime.strptime(from_date, configs.YM_DATE)
            datetime.strptime(to_date, configs.YM_DATE)
        except ValueError:
            return jsonify({
                'status': False,
                'message': 'Invalid date format. Use YYYY-MM-DD',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        district_condition = ""
        if (view_role == 3 or view_role == 4) and district_ids:
            district_condition = f"AND district_id IN ({', '.join(map(str, district_ids))})"
        elif view_role == 5 and district_ids:
            # district_condition = f"AND district_id IN ({', '.join(map(str, district_ids))}) AND police_station IN ('{', '.join(map(str, police_stations))}')"
            district_condition = (
                f"AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        # Query to get district-wise case counts
        cases_query = """
            SELECT
                district_id, 
                COUNT(*) AS total_cases
            FROM 
                response_time
            WHERE 
                district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
                AND (
                    level1_case_nature IN ('Crime Against Person', 'Crime Against Property') 
                    OR level3_case_nature IN ('Aerial Firing', 'Attempt to Illegal Possession of Land/ Premises')
                )
                AND district_id IS NOT NULL
                AND date BETWEEN %s AND %s
                {district_condition}
                AND parent_id = 0
                AND response_time IS NOT NULL
                AND response_time > 0
                AND district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
            GROUP BY 
                district_id;
        """
        cases_query = cases_query.format(district_condition=district_condition)
        processed_db_cursor.execute(cases_query, (from_date, to_date))
        cases_summary = processed_db_cursor.fetchall()

        # Process and format the response data
        district_stats = {
            configs.DISTRICTS_DICTIONARY.get(int(district_id)): count
            for district_id, count in cases_summary
        }

        response = {
            'status': True,
            'message': 'District-wise statistics fetched successfully',
            'data': district_stats
        }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': 'Internal server error',
            'data': None
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)

        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.DISTRICTWISE_MORE_INFO['ENDPOINT'], methods=['POST'])  # Changed to POST
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def districtwise_more_info():
    """Get detailed district-wise information including category-wise stats, police station stats, and cases.

    Expected POST body:
    {
        "fromDate": "YYYY-MM-DD",  # Optional
        "toDate": "YYYY-MM-DD",   # Optional
        "view_role": int,         # User role ID (2 = All data, 3,4 = District-specific data & 5 = PS specific Data)
        "district":  str          # String of districts
        "police_station" : str    # String of police_stations
    }

    Returns:
        JSON: Detailed district statistics
    """
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()

    try:
        # Get and validate request body
        district_str = request.form.get('district')
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        view_role = request.form.get('view_role', type=int)
        police_station_str = request.form.get('police_station')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        if not all([district_str, from_date, to_date]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        if view_role == 5 and district_ids:
            district_condition = (
                f"AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        else:
            district_condition = f"AND district_id = {configs.REVERSED_DISTRICTS_DICTIONARY.get(district_str)}"

        # For PS Query
        if view_role == 5 and district_ids:
            additional_condition = (
                f"AND rt.district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND rt.police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        else:
            additional_condition = f"AND rt.district_id = {configs.REVERSED_DISTRICTS_DICTIONARY.get(district_str)}"

        # Get category-wise statistics
        category_query = """
            SELECT 
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Murder', 'Attempt to Murder')
                ) AS murder,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Highway/Road/Street Dacoity', 'House Dacoity', 'Any Other Dacoity', 
                        'Shop Dacoity', 'Cattle Dacoity', 'Patrol Pump Dacoity', 'Jewellery Shop Dacoity'
                    )
                ) AS dacoity,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Aerial Firing')
                ) AS aerial_firing,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Sexual Assault/ Harrasment To Women')
                ) AS rape,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Child Kidnapping', 'Female Kidnapping/ Abduction', 'Male Kidnapping/ Abduction',
                        'Kidnapping for Ransom', 'Attempt to Kidnap / Abduct', 'Child Kidnapping '
                    )
                ) AS kidnapping,
                COUNT(*) FILTER (
                    WHERE queue = 'minorities-15'
                ) AS minorities,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Highway/Road/Street Robbery', 'Any Other Robbery', 'Cattle Robbery', 'House Robbery',
                        'Shop Robbery', 'Patrol Pump Robbery', 'Bank/Money Exchange/ ATM Robbery', 'Car Snatching',
                        'Other Vehicles Snatching', 'Snatching/Jhapatta', 'Motorcycle Snatching',
                        'Jewellery Shop Robbery', 'Robbery with Murder'
                    )
                ) AS robbery_snatching,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Motorcycle Theft')
                ) AS motorcycle_theft,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Car Theft')
                ) AS car_theft,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Mobile Theft', 'Any Other Theft', 'Cattle theft', 'Transformer/ Motor Theft', 
                        'Pick Pocketing', 'Purse / Wallet / Luggage Theft', 'Cycle Theft', 'Weapon Theft', 
                        'Other Vehicles Theft', 'House Burglary', 'Shop Burglary', 'Other Burglary'
                    )
                ) AS theft,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Rape', 'Child Abuse / Molestation')
                ) AS child_abuse,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Firing on Police', 'Suicidal Attack/ Bomb Blast/ Terrorist Attack')
                ) AS terrorist_act,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Prostitution/ Brothel House', 'Acid Throwing', 'Hurt / Injuries', 'Street Fight',
                        'Other Assault', 'Physical Threats / Harrasment', 'Domestic Violence',
                        'Criminal Intimidation (Threat with Weapon)'
                    )
                ) AS other_person,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Attempt to Illegal Possession of Land/ Premises')
                ) AS other_property,
                COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Dacoity with Murder')
                ) AS dacoity_with_murder
            FROM response_time
            WHERE district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
              AND district_id IS NOT NULL
              AND (
                    level1_case_nature IN ('Crime Against Person', 'Crime Against Property') 
                    OR level3_case_nature IN ('Aerial Firing', 'Attempt to Illegal Possession of Land/ Premises')
                )
              AND date BETWEEN %s AND %s
              {district_condition}
              AND parent_id = 0
              AND response_time IS NOT NULL
              AND response_time > 0;

        """
        # Dynamically format the query to include `district_condition`
        category_query = category_query.format(district_condition=district_condition)
        processed_db_cursor.execute(category_query, (from_date, to_date))

        (murder, dacoity, firing, rape_sexual_assault, kidnapping,
         minorities, robbery_snatching, motorcycle_theft, car_theft, theft,
         child_abuse, terrorism, other_person, other_property, dacoity_with_murder) = processed_db_cursor.fetchone()

        categorywise_response = {
            'terrorism': terrorism,
            'murder': murder,
            'kidnapping': kidnapping,
            'minorities': minorities,
            'dacoity': dacoity,  # To be removed
            'robbery_snatching': robbery_snatching,
            'theft': theft,
            'child_abuse': child_abuse,
            'motorcycle_theft': motorcycle_theft,
            'car_theft': car_theft,
            'Illegal Possession': other_property,
            'other': other_person,
            'environment_smog_issues': 0,
            'firing': firing,
            'sexual_assault': rape_sexual_assault,
            'dacoity_with_murder': dacoity_with_murder,
            'police_encounter': 0
        }

        # Get police station-wise statistics
        ps_query = f"""
            SELECT 
                ps.police_circle, 
                ps.police_station, 
                COALESCE(COUNT(rt.lead_id), 0) AS total_count
            FROM 
                (SELECT DISTINCT 
                    police_circle, 
                    police_station, 
                    police_station_id 
                 FROM 
                    response_time 
                 WHERE 
                    police_station IS NOT NULL 
                    {district_condition}
                ) ps
            LEFT JOIN 
                response_time rt 
            ON 
                ps.police_station_id = rt.police_station_id
                AND rt.district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
                AND rt.date BETWEEN %s AND %s
                {additional_condition}
                AND (
                    rt.level1_case_nature IN ('Crime Against Person', 'Crime Against Property') 
                    OR rt.level3_case_nature = 'Aerial Firing'
                    OR rt.level3_case_nature = 'Attempt to Illegal Possession of Land/ Premises'
                )
                AND rt.parent_id = 0
                AND rt.police_station IS NOT NULL
            GROUP BY 
                ps.police_circle, 
                ps.police_station
            ORDER BY 
                ps.police_circle DESC;
        """
        # Dynamically format the query to include conditions
        ps_query = ps_query.format(district_condition=district_condition, additional_condition=additional_condition)

        processed_db_cursor.execute(ps_query, (from_date, to_date))
        ps_wise_count = processed_db_cursor.fetchall()

        GENERIC_DIVISION_NAME = "No Division"

        ps_wise_response_dict = {}

        for row in ps_wise_count:
            police_circle = row[0]
            police_station = row[1]
            count = row[2]

            if district_str == 'Multan':
                police_division = configs.MULTAN_DIVISON_MAPPING[police_circle]
            elif district_str == 'Lahore':
                if police_station == 'Sabzazar':
                    police_division = 'Iqbal Town Division'
                else:
                    police_division = configs.LAHORE_DIVISION_MAPPING[police_circle]
            elif district_str == 'Rawalpindi':
                if police_circle in configs.RAWALPINDI_DIVISION_MAPPING:
                    police_division = configs.RAWALPINDI_DIVISION_MAPPING[police_circle]
                else:
                    continue
            elif district_str == 'Gujranwala':
                police_division = configs.GUJRANWALA_DIVISION_MAPPING[police_circle]
            elif district_str == 'Faisalabad':
                if police_circle in configs.FAISALABAD_DIVISION_MAPPING:
                    police_division = configs.FAISALABAD_DIVISION_MAPPING[police_circle]
                else:
                    continue
            else:
                # Use a generic division
                police_division = GENERIC_DIVISION_NAME

            # Initialize nested dictionaries if not already present
            if police_division not in ps_wise_response_dict:
                ps_wise_response_dict[police_division] = {}

            # Handle specific renaming case for 'Sadar F/abad'
            if district_str == 'Faisalabad' and police_circle == 'Sadar F/abad':
                corrected_circle = 'Sadar'
                if corrected_circle not in ps_wise_response_dict[police_division]:
                    ps_wise_response_dict[police_division][corrected_circle] = {}
                ps_wise_response_dict[police_division][corrected_circle][police_station] = count
            else:
                if police_circle:
                    if police_circle not in ps_wise_response_dict[police_division]:
                        ps_wise_response_dict[police_division][police_circle] = {}
                    ps_wise_response_dict[police_division][police_circle][police_station] = count
                else:
                    police_circle = 'Sabzazar'
                    if police_circle not in ps_wise_response_dict[police_division]:
                        ps_wise_response_dict[police_division][police_circle] = {}
                    ps_wise_response_dict[police_division][police_circle][police_station] = count

        response = {
            'status': True,
            'message': 'District details fetched successfully',
            'data': {
                'categorywise_response': categorywise_response,
                'ps_wise_response': ps_wise_response_dict
            }
        }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': f'Internal server error {traceback.format_exc()}'
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)

        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.PSWISE_CATEGORIES['ENDPOINT'], methods=[configs.PSWISE_CATEGORIES['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def pswise_categories():
    """Get police station-wise crime statistics and case details.

    Expected POST body:
    {
        "fromDate": "YYYY-MM-DD",
        "toDate": "YYYY-MM-DD",
        "police_station" : Police Station Name
        "district": "District Name"
    }

    Returns:
        JSON: Police station statistics and cases
    """
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()

    try:
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        police_station = request.form.get('police_station')
        district = request.form.get('district')

        if not all([from_date, to_date, police_station, district]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        district_id = configs.REVERSED_DISTRICTS_DICTIONARY[district]
        if not district_id:
            return jsonify({
                'status': False,
                'message': 'Invalid district name',
                'data': None
            }), 400

        ps_query = """
            SELECT 
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Murder', 'Attempt to Murder')
                ), 0) AS murder,
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Highway/Road/Street Dacoity', 'House Dacoity', 'Any Other Dacoity', 
                        'Shop Dacoity', 'Cattle Dacoity', 'Patrol Pump Dacoity', 'Jewellery Shop Dacoity'
                    )
                ), 0) AS dacoity,
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature = 'Aerial Firing'
                ), 0) AS aerial_firing,
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Sexual Assault/ Harrasment To Women')
                ), 0) AS rape,
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Child Kidnapping', 'Female Kidnapping/ Abduction', 'Male Kidnapping/ Abduction',
                        'Kidnapping for Ransom', 'Attempt to Kidnap / Abduct', 'Child Kidnapping '
                    )
                ), 0) AS kidnapping,
                COALESCE(COUNT(*) FILTER (
                    WHERE queue = 'minorities-15'
                ), 0) AS minorities,
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Highway/Road/Street Robbery', 'Any Other Robbery', 'Cattle Robbery', 'House Robbery',
                        'Shop Robbery', 'Patrol Pump Robbery', 'Bank/Money Exchange/ ATM Robbery', 'Car Snatching',
                        'Other Vehicles Snatching', 'Snatching/Jhapatta', 'Motorcycle Snatching',
                        'Jewellery Shop Robbery', 'Robbery with Murder'
                    )
                ), 0) AS robbery_snatching,
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature = 'Motorcycle Theft'
                ), 0) AS motorcycle_theft,
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature = 'Car Theft'
                ), 0) AS car_theft,
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Mobile Theft', 'Any Other Theft', 'Cattle theft', 'Transformer/ Motor Theft', 
                        'Pick Pocketing', 'Purse / Wallet / Luggage Theft', 'Cycle Theft', 'Weapon Theft', 
                        'Other Vehicles Theft', 'House Burglary', 'Shop Burglary', 'Other Burglary'
                    )
                ), 0) AS theft,
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Rape', 'Child Abuse / Molestation')
                ), 0) AS child_abuse,
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Firing on Police', 'Suicidal Attack/ Bomb Blast/ Terrorist Attack')
                ), 0) AS terrorist_act,
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature IN (
                        'Prostitution/ Brothel House', 'Acid Throwing', 'Hurt / Injuries', 'Street Fight',
                        'Other Assault', 'Physical Threats / Harrasment', 'Domestic Violence', 
                        'Criminal Intimidation (Threat with Weapon)'
                    )
                ), 0) AS other_person,
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Attempt to Illegal Possession of Land/ Premises')
                ), 0) AS other_property,
                COALESCE(COUNT(*) FILTER (
                    WHERE level3_case_nature IN ('Dacoity with Murder')
                ), 0) AS dacoity_with_murder
            FROM response_time
            WHERE date BETWEEN %s AND %s
              AND district_id = %s
              AND police_station = %s
              AND (
                    level1_case_nature IN ('Crime Against Person', 'Crime Against Property') 
                    OR level3_case_nature IN ('Aerial Firing', 'Attempt to Illegal Possession of Land/ Premises')
                )
              AND parent_id = 0
              AND response_time IS NOT NULL
              AND response_time > 0;
        """
        # Execute the query
        processed_db_cursor.execute(ps_query, (from_date, to_date, district_id, police_station))
        (murder, dacoity, firing, rape_sexual_assault, kidnapping, minorities,
         robbery_snatching, motorcycle_theft, car_theft, theft, child_abuse,
         terrorism, other_person, other_property,
         dacoity_with_murder) = processed_db_cursor.fetchone()

        pswise_response = {
            'terrorism': terrorism,
            'murder': murder,
            'Illegal_possession': other_property,
            'kidnapping': kidnapping,
            'minorities': minorities,
            'other': other_person,
            'dacoity': dacoity,  # to be removed
            'robbery_snatching': robbery_snatching,
            'theft': theft,
            'child_abuse': child_abuse,
            'motorcycle_theft': motorcycle_theft,
            'car_theft': car_theft,
            'firing': firing,
            'police_encounter': 0,
            'women_harrassment': rape_sexual_assault,
            'dacoity_with_murder': dacoity_with_murder,
            'environmental_smog_issues': 0
        }

        # Get case details
        cases_query = """
            SELECT 
                case_number, level3_case_nature, caller_name, caller_number,
                accepted_time, police_station, district_id, time_id,
                description, first_arrival_time, response_time
            FROM response_time
            WHERE police_station = %s
                AND district_id = %s
                AND date BETWEEN %s AND %s
                AND (level1_case_nature in ('Crime Against Person','Crime Against Property') OR level3_case_nature = 'Aerial Firing'
                    OR level3_case_nature = 'Attempt to Illegal Possession of Land/ Premises')
                AND district_id NOT IN ('0','41','42','43','44','45','46')
                AND police_station is not Null
                AND parent_id = 0
        """
        processed_db_cursor.execute(cases_query, (police_station, district_id, from_date, to_date))
        cases = processed_db_cursor.fetchall()

        cases_list = [
            {
                "case_number": case_number,
                "case_nature": level3_case_nature,
                "caller_name": caller_name,
                "assigned_time": created_time,
                "cli": caller_number,
                "police_station": police_station,
                "status": 'Completed',
                "district": configs.DISTRICTS_DICTIONARY.get(int(district_id)),
                "time_id": datetime.fromtimestamp(int(time_id)).strftime(configs.YMD_HMS),
                "description": description,
                "reached_time": datetime.fromtimestamp(int(reached_time)).strftime(
                    configs.YMD_HMS) if reached_time else None,
                "response_time": f"{int(response_time // 60)}:{int(response_time % 60):02d}" if response_time else 0
            }
            for
            case_number, level3_case_nature, caller_name,
            caller_number, created_time, police_station,
            district_id, time_id, description, reached_time, response_time
            in cases
        ]

        generated_cases_query = """
                    select count(*)
                    from response_time
                    where date between %s and %s
                    AND district_id = %s
                    AND police_station = %s
                    AND parent_id = 0
                    AND (level1_case_nature in ('Crime Against Person','Crime Against Property') OR level3_case_nature = 'Aerial Firing'
                    OR level3_case_nature = 'Attempt to Illegal Possession of Land/ Premises')
        """
        processed_db_cursor.execute(generated_cases_query, (from_date, to_date, district_id, police_station))
        generated_case_count = processed_db_cursor.fetchone()[0]

        fir_count_query = """
                    select sum(fir_count)
                    from fir_data
                    where date between %s and %s
                    AND district_id = %s
                    AND police_station = %s
        """
        processed_db_cursor.execute(fir_count_query, (from_date, to_date, district_id, police_station))
        fir_count = processed_db_cursor.fetchone()[0]

        ps_responsetime_query = """
                    select AVG(response_time)
                    from response_time
                    where date between %s and %s
                    AND district_id = %s
                    AND police_station = %s
                    AND parent_id = 0
                    AND (level1_case_nature in ('Crime Against Person','Crime Against Property') OR level3_case_nature = 'Aerial Firing'
                    OR level3_case_nature = 'Attempt to Illegal Possession of Land/ Premises')
        """
        processed_db_cursor.execute(ps_responsetime_query, (from_date, to_date, district_id, police_station))
        ps_response_time = processed_db_cursor.fetchone()[0]
        response_time = f"{int(ps_response_time // 60)}:{int(ps_response_time % 60):02d}" if ps_response_time is not None and round(
            ps_response_time, 2) else 0

        conf_calls_query = """
                    select count(*)
                    from response_time
                    where date between %s and %s
                    AND district_id = %s
                    AND police_station = %s
                    AND field3 IN ('Successful Conference call','FO did not attend the call','Number Powered Off','Out of PS Jurisdiction')
                    AND parent_id = 0
                    AND (level1_case_nature in ('Crime Against Person','Crime Against Property') OR level3_case_nature = 'Aerial Firing'
                    OR level3_case_nature = 'Attempt to Illegal Possession of Land/ Premises')
        """
        processed_db_cursor.execute(conf_calls_query, (from_date, to_date, district_id, police_station))
        conf_calls = processed_db_cursor.fetchone()[0]

        conference_call_query = f"""
                                SELECT 
                                    SUM(CASE WHEN field3 = 'Successful Conference call' THEN 1 ELSE 0 END) AS successful_calls,
                                    SUM(CASE WHEN field3 IN ('FO did not attend the call','Number Powered Off','Out of PS Jurisdiction') THEN 1 ELSE 0 END) AS Unsuccessful_calls
                                FROM
                                    response_time
                                WHERE 
                                    field3 IS NOT NULL
                                    AND date BETWEEN %s AND %s
                                    AND district_id is not NULL
                                    AND parent_id = 0
                                    AND (level1_case_nature in ('Crime Against Person','Crime Against Property') OR level3_case_nature = 'Aerial Firing'
                                        OR level3_case_nature = 'Attempt to Illegal Possession of Land/ Premises')
                                    AND district_id = %s
                                    AND police_station = %s
                                """
        processed_db_cursor.execute(conference_call_query, (from_date, to_date, district_id, police_station))
        (successful_calls, unsuccessful_calls) = processed_db_cursor.fetchone()

        vehicles_query = """
                    SELECT latitude, longitude, name, phone_number,
                    status, police_station, district, registration_no
                    FROM police_mv_locations
                    WHERE district = %s
                    AND police_station = %s
        """
        processed_db_cursor.execute(vehicles_query, (district, police_station))
        vehicles_location = processed_db_cursor.fetchall()

        column_names = [
            "latitude", "longitude", "name", "phone",
            "status", "police_station", "district", "vehicle"
        ]

        vehicles_location_data = [dict(zip(column_names, vehicle)) for vehicle in vehicles_location]

        crime_hotspots_query = """
                            Select district_id ,police_station, level2_case_nature , reached_lat, reached_long
                            FROM response_time
                            WHERE district_id = %s
                            AND police_station = %s
                            AND reached_lat is NOT NULL
                            AND reached_long is NOT NULL
                                """

        processed_db_cursor.execute(crime_hotspots_query, (district_id, police_station))
        hotspot_results = processed_db_cursor.fetchall()

        # Initialize an empty list to store the coordinates
        coordinates = []

        # Loop through the fetched results
        for result in hotspot_results:
            # Extract the latitude and longitude values from the result
            reached_lat = result[3]
            reached_long = result[4]

            # Append the coordinates as a list to the coordinates list
            coordinates.append([float(reached_lat), float(reached_long)])

        # successful response
        response = {
            'status': True,
            'message': 'Police station statistics fetched successfully',
            'data': {
                'pswise_response': pswise_response,
                'cases_list': cases_list,
                'total_cases': generated_case_count,
                'total_firs': fir_count if fir_count else 0,
                'response_time': response_time,
                'conference_calls': conf_calls,
                'police_mv_locations': vehicles_location_data,
                'successful_conf_calls': successful_calls,
                'unsuccessful_conf_calls': unsuccessful_calls,
                'hotspot_coordinates': filter_lat_longs(coordinates, max_distance_km=3)
            }
        }
        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': 'Internal server error',
            'data': None
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)

        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.PUNJABTODAY_CASE_DETAILS['ENDPOINT'],
           methods=[configs.PUNJABTODAY_CASE_DETAILS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def punjab_case_details():
    """Get detailed information for a specific case number.

    Expected POST body:
    {
        "case_number": "string",      # The case number to retrieve details for
        "assigned_to": "string",      # Optional; if empty, use the current username or handle differently
        "assigned_by": "string"       # Optional; if empty, use the current username or handle differently
    }

    Returns:
        JSON: Case details with a standardized response format.
              When neither assigned parameter is provided, an additional key
              'assigned_by_users' lists all users to whom the current user has assigned the case.
    """
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()

    try:
        username = get_jwt_identity()

        case_number_str = str(request.form.get('case_number'))
        assigned_to_param = request.form.get('assigned_to')
        assigned_by_param = request.form.get('assigned_by')

        case_query = """
            SELECT 
                case_number, description, response_time, 
                responder_id, accepted_time, police_station,
                district_id, region_category, caller_name,
                caller_number, caller_location, level3_case_nature, 
                dispatched_time, first_arrival_time,lat, long, 
                responder_lat, responder_long
            FROM response_time
            WHERE case_number = %s
        """
        processed_db_cursor.execute(case_query, (case_number_str,))
        case_details = processed_db_cursor.fetchone()

        if not case_details:
            return jsonify({
                'status': False,
                'message': 'Case not found',
                'data': None
            }), 404

        # Unpack the case details
        (case_number, description, response_time, responder_id, accepted_time, police_station,
         district_id, region_category, caller_name, caller_number, caller_location,
         level3_case_nature, dispatched_time, first_arrival_time,
         lat, long, responder_lat, responder_long) = case_details

        # Query remarks (if applicable) or get assigned users list ---
        # Initialize variables that will be used in the response.
        new_remarks = ""
        new_assignedby = None
        new_assignedto = None
        assigned_users = None  # extra key added only in one scenario

        # Neither assigned_to nor assigned_by provided.
        if not assigned_to_param and not assigned_by_param:
            assigned_query = """
                SELECT DISTINCT assigned_to
                FROM remarks
                WHERE case_id = %s AND assigned_by = %s
            """
            processed_db_cursor.execute(assigned_query, (case_number_str, username))
            assigned_records = processed_db_cursor.fetchall()
            assigned_users = [rec[0] for rec in assigned_records] if assigned_records else []
            # For this example, we leave remarks empty and record that the current user is the assigner.
            new_assignedby = username
            new_assignedto = None
        else:
            # one of assigned_to or assigned_by is provided.
            if not assigned_to_param:
                assigned_to = username
                assigned_by = assigned_by_param
            elif not assigned_by_param:
                assigned_by = username
                assigned_to = assigned_to_param
            else:
                assigned_to = assigned_to_param
                assigned_by = assigned_by_param

            remarks_query = """
                SELECT remarks, assigned_by, assigned_to
                FROM remarks
                WHERE case_id = %s AND assigned_to = %s AND assigned_by = %s
                LIMIT 1
            """
            processed_db_cursor.execute(remarks_query, (case_number_str, assigned_to, assigned_by))
            remarks_record = processed_db_cursor.fetchone()

            if remarks_record:
                new_remarks = remarks_record[0]
                new_assignedby = remarks_record[1]
                new_assignedto = remarks_record[2]
            else:
                new_remarks = ""
                new_assignedby = assigned_by
                new_assignedto = assigned_to

        case_response = {
            'case_number': case_number,
            'description': description,
            'response_time': f"{int(response_time // 60)}:{int(response_time % 60):02d}" if response_time else "0:00",
            'responder_id': responder_id,
            'accepted_time': accepted_time,
            'police_station': police_station,
            'district_id': configs.DISTRICTS_DICTIONARY.get(district_id),
            'region_category': region_category,
            'caller_name': caller_name,
            'caller_number': caller_number,
            'caller_location': caller_location,
            'level3_case_nature': level3_case_nature,
            'remarks': utils.parse_remarks(new_remarks),
            'assigned_by': new_assignedby,
            'assigned_to': new_assignedto,
            'dispatched_time': (datetime.fromtimestamp(int(dispatched_time)).strftime("%d %b %Y %H:%M:%S")
                                if dispatched_time is not None else 'N/A'),
            'first_arrival_time': (datetime.fromtimestamp(int(first_arrival_time)).strftime("%d %b %Y %H:%M:%S")
                                   if first_arrival_time is not None else 'N/A'),
            'lat': lat,
            'long': long,
            'responder_lat': responder_lat,
            'responder_long': responder_long
        }

        # incase assigned_by and assigned_to are not provided
        if assigned_users is not None:
            case_response['assigned_by_users'] = assigned_users

        response = {
            'status': True,
            'message': 'Case details fetched successfully',
            'data': case_response
        }
        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': f'Internal server error: {e}',
            'data': None
        }), 500

    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)

        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.DIST_CATEGORY_DETAILS['ENDPOINT'], methods=[configs.DIST_CATEGORY_DETAILS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def district_category_details():
    """Get district-wise details for a specific crime category.

    Expected POST body:
    {
        "fromDate": "YYYY-MM-DD",
        "toDate": "YYYY-MM-DD",
        "district": "District Name",
        "category": "Category Name"
        "view_role": int,         # User role ID (2 = All data, 3,4 = District-specific data & 5 = PS specific Data)
        "police_station" : str    # String of police_stations
    }

    Returns:
        JSON: Police station-wise counts and case details for the specified category
    """
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()

    try:
        # Get and validate request body
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        district_str = request.form.get('district')
        category = request.form.get('category')
        view_role = request.form.get('view_role', type=int)
        police_station_str = request.form.get('police_station')

        if not all([from_date, to_date, district_str, category]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters: fromDate, toDate, district, category',
                'data': None
            }), 400

        category = "other_property" if category == "Illegal Possession" else category
        category = "rape" if category == "sexual_assault" else category
        category = "aerial_firing" if category == "firing" else category

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        district_condition = ""
        if view_role == 5 and district_ids:
            district_condition = (
                f"AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        else:
            district_condition = f"AND district_id IN ({', '.join(map(str, district_ids))})"
        # query conditions based on category
        base_query = """
            SELECT 
                case_number, level3_case_nature, caller_name, caller_number,
                accepted_time, police_station, district_id, time_id,
                description, first_arrival_time, response_time
            FROM response_time
            WHERE {condition}
                AND district_id NOT IN ('0','41','42','43','44','45','46')
                AND police_station is not Null
                AND date BETWEEN %s AND %s
                {district_condition}
                AND parent_id = 0
                AND response_time IS NOT NULL
                AND response_time > 0
        """

        # For PS Query
        if view_role == 5 and district_ids:
            additional_condition = (
                f"AND rt.district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND rt.police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        else:
            additional_condition = f"AND rt.district_id = {configs.REVERSED_DISTRICTS_DICTIONARY.get(district_str)}"

        if category == 'minorities':
            where_condition = "queue = 'minorities-15'"
            query_params = [from_date, to_date]
        else:
            categories = configs.CATEGORIES.get(category)
            where_condition = f"level3_case_nature IN ({','.join(['%s'] * len(categories))})"
            query_params = categories + [from_date, to_date]

        # Get case details
        cases_query = base_query.format(condition=where_condition, district_condition=district_condition)
        processed_db_cursor.execute(cases_query, query_params)
        cases = processed_db_cursor.fetchall()

        # cases details
        cases_list = [
            {
                "case_number": case_number,
                "case_nature": level3_case_nature,
                "caller_name": caller_name,
                "assigned_time": created_time,
                "cli": caller_number,
                "police_station": police_station,
                "status": 'Completed',
                "district": configs.DISTRICTS_DICTIONARY.get(district_id),
                "time_id": datetime.fromtimestamp(int(time_id)).strftime(configs.YMD_HMS),
                "description": description,
                "reached_time": datetime.fromtimestamp(int(reached_time)).strftime(
                    configs.YMD_HMS) if reached_time else None,
                "response_time": f"{int(response_time // 60)}:{int(response_time % 60):02d}" if response_time else 0
            }
            for (case_number, level3_case_nature, caller_name, caller_number,
                 created_time, police_station, district_id, time_id,
                 description, reached_time, response_time) in cases
        ]

        # Get police station-wise counts
        counts_query = """
                    SELECT 
                        ps.police_station,
                        COALESCE(COUNT(rt.lead_id), 0) AS total_count
                    FROM
                        (SELECT DISTINCT police_station_id, police_station 
                         FROM response_time 
                         WHERE police_station is NOT NULL {district_condition}) ps
                    LEFT JOIN
                        response_time rt
                    ON
                        ps.police_station_id = rt.police_station_id
                        AND {condition}
                        AND rt.date BETWEEN %s AND %s
                        {additional_condition}
                        AND rt.parent_id = 0
                        AND rt.police_station is NOT NULL
                        AND rt.response_time IS NOT NULL
                        AND rt.response_time > 0
                    GROUP BY
                        ps.police_station_id, ps.police_station;
        """

        ps_query = counts_query.format(condition=where_condition, district_condition=district_condition,
                                       additional_condition=additional_condition)
        processed_db_cursor.execute(ps_query, query_params)
        counts = processed_db_cursor.fetchall()

        ps_counts = {ps: count for ps, count in counts}

        # successful response
        response = {
            'status': True,
            'message': 'District category details fetched successfully',
            'data': {
                'total_cases': len(cases_list),
                'ps_counts': ps_counts,
                'cases': cases_list

            }
        }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': f'Internal server error {e}',
            'data': None
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)

        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.PREDICTIVE_FORECAST['ENDPOINT'], methods=[configs.PREDICTIVE_FORECAST['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def forcast_predictive_policing():
    try:
        log_db_conn, log_db_cursor = get_log_pg_db_connection()

        ps = request.form.get('police_station')
        district = request.form.get('district')
        forecast = yesterday_forecast(ps, district)
        dashboard_data = get_category_data(ps, district)

        data = {**forecast, **dashboard_data}

        response = {
            'status': True,
            'message': 'Prediction Comparison fetched successfully',
            'data': data
        }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': 'Internal server error',
            'data': None
        }), 500
    finally:
        if log_db_cursor:
            log_db_cursor.close()

        if log_db_conn:
            log_db_conn.rollback()  # Rollback any uncommitted transactions
            log_db_conn.close()  # Properly return to the pool without removing it


@app.route(configs.DATEWISE_FORECAST['ENDPOINT'], methods=[configs.DATEWISE_FORECAST['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def forecast_datewise():
    try:
        log_db_conn, log_db_cursor = get_log_pg_db_connection()

        ps = request.form.get('police_station')
        district = request.form.get('district')
        start_date_str = request.form.get('start_date')
        end_date_str = request.form.get('end_date')

        if not all([start_date_str, end_date_str]):
            return jsonify({"error": "Missing required params"}), 400

        start_date = datetime.strptime(start_date_str, '%Y-%m-%d').strftime('%d-%m-%Y')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d').strftime('%d-%m-%Y')

        data = forecast_date(ps, district, start_date, end_date)

        response = {
            'status': True,
            'message': 'Datewise Prediction fetched successfully',
            'data': data
        }
        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': f'Internal server error : {e}',
            'data': None
        }), 500
    finally:
        if log_db_cursor:
            log_db_cursor.close()

        if log_db_conn:
            log_db_conn.rollback()  # Rollback any uncommitted transactions
            log_db_conn.close()  # Properly return to the pool without removing it


@app.route(configs.EMERGENCY_15_INTEGRATION['ENDPOINT'], methods=[configs.EMERGENCY_15_INTEGRATION['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def emergency_15_integration():
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    try:
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        district_str = request.form.get('district')
        view_role = request.form.get('view_role', type=int)
        police_station_str = request.form.get('police_station')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                    }), 400

        #################################################### EMERGENCY 15 STATS
        processed_query = utils.build_processed_data_query(configs.PROCESSED_COLUMNS)

        district_condition = ""
        if (view_role == 3 or view_role == 4) and district_ids:
            district_condition = f"AND district_id IN ({', '.join(map(str, district_ids))})"
            processed_query += district_condition
        elif view_role == 5 and district_ids:
            district_condition = (
                f"AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
            processed_query += district_condition

        params = []
        processed_query += " AND date BETWEEN %s AND %s"
        params.extend([from_date, to_date])

        processed_db_cursor.execute(processed_query, params)
        row = processed_db_cursor.fetchone()

        (total_calls, siraiki, punjabi, potohari, english, traffic,
         vwps, app_alerts, transfered, call_backs, video_calls,
         estimated_response_time, succ_conf_calls, unsucc_conf_calls,
         vccs, vcm, generated_cases) = row
        emergency_15 = (total_calls - vwps - traffic - vccs - vcm) if total_calls else None

        if isinstance(estimated_response_time, Decimal):
            estimated_response_time = float(estimated_response_time)

        conference_call_query = """
                        SELECT 
                            SUM(CASE WHEN field3 = 'Successful Conference call' THEN 1 ELSE 0 END) AS successful_calls,
                            SUM(CASE WHEN field3 IN ('FO did not attend the call','Number Powered Off') THEN 1 ELSE 0 END) AS Unsuccessful_calls
                        FROM 
                            response_time
                        WHERE 
                            field3 IS NOT NULL
                            AND date BETWEEN %s AND %s
                            AND district_id is not NULL
                            AND parent_id = 0
                            {district_condition}
                        """
        conference_call_query = conference_call_query.format(district_condition=district_condition)

        processed_db_cursor.execute(conference_call_query, (from_date, to_date))
        (successful_calls, unsuccessful_calls) = processed_db_cursor.fetchone()

        fir_query = """
                        SELECT 
                            SUM(fir_count)
                        FROM 
                            fir_data
                        WHERE 
                            date BETWEEN %s AND %s
                            {district_condition}
                        """
        fir_query = fir_query.format(district_condition=district_condition)

        processed_db_cursor.execute(fir_query, (from_date, to_date))
        fir_count = processed_db_cursor.fetchone()[0]

        emergency_15_stats = {
            'total_calls': total_calls,
            'female-15_count': vwps,
            'traffic-15_count': traffic,
            'pucar-15_count': emergency_15,
            'total_generated_cases': generated_cases,
            'vccs-15_count': vccs,
            'vcm-15_count': vcm,
            'successful_conference_calls': successful_calls,
            'unsuccessful_conference_calls': unsuccessful_calls,
            'fir_count': fir_count
        }

        #################################################### Police 15 Vehicle Stats
        other_condition = ""
        if (view_role == 3 or view_role == 4) and districts:
            other_condition = f"AND district IN ({', '.join(repr(district) for district in districts)}) "
        elif view_role == 5 and districts:
            other_condition = (
                f"AND district IN ({', '.join(repr(district) for district in districts)}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )

        police_mv_stats_query = """
                        WITH statuses AS (
                            SELECT 'Free' AS status
                            UNION ALL
                            SELECT 'Idle'
                            UNION ALL
                            SELECT 'Busy'
                        )
                        SELECT 
                            s.status,
                            COALESCE(COUNT(p.status), 0) AS count
                        FROM 
                            statuses s
                        LEFT JOIN 
                            police_mv_locations p
                        ON 
                            s.status = p.status
                        WHERE 
                            1=1
                            {other_condition}
                        GROUP BY 
                            s.status

                        UNION ALL

                        SELECT 
                            'Total' AS status,
                            COUNT(*) AS count
                        FROM 
                            police_mv_locations
                        WHERE 
                            1=1
                            {other_condition}
        """
        police_mv_stats_query = police_mv_stats_query.format(other_condition=other_condition)

        processed_db_cursor.execute(police_mv_stats_query, )
        vehicle_counts = processed_db_cursor.fetchall()

        if not vehicle_counts:
            status_counts = {}
        else:
            status_counts = {row[0]: row[1] for row in vehicle_counts}

        ############################################## Police MV locations Data
        police_mv_locations_query = """
                            SELECT 
                                name , police_station , latitude , longitude, 
                                registration_no, district , unit_type 
                            FROM 
                                police_mv_locations
                            WHERE 
                            1=1
                            {other_condition}
                """
        police_mv_locations_query = police_mv_locations_query.format(other_condition=other_condition)

        processed_db_cursor.execute(police_mv_locations_query, )
        records = processed_db_cursor.fetchall()

        columns = ["name", "police_station", "latitude", "longitude",
                   "registration_no", "district", "unit_type"]
        mv_data = [dict(zip(columns, row)) for row in records]

        urdu_districts = [configs.district_eng_urdu.get(district, district) for district in districts]

        homicide_fir_query = utils.construct_homicide_fir_query(urdu_districts)

        processed_db_cursor.execute(homicide_fir_query)
        lat_long_records = processed_db_cursor.fetchall()

        # Convert query results into a list of [latitude, longitude] pairs
        district_lat_long = [[row[0], row[1]] for row in lat_long_records]

        response = {
            'status': True,
            'message': 'Integrated Emergency 15 Dashboard details fetched successfully',
            'data': {
                'emergency_15_stats': emergency_15_stats,
                'vehicle_stats': status_counts,
                'vehicles_data': mv_data,
                # 'homicide_hotspots': utils.filter_lat_longs(district_lat_long, district_str)
                'homicide_hotspots': district_lat_long
            }
        }
        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': f'Internal server error {e}',
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
         # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.ADD_REMARKS['ENDPOINT'], methods=[configs.ADD_REMARKS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def add_remarks():
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    notification_conn, notification_cursor = get_notification_db_connection()
    log_db_conn, log_db_cursor = get_log_pg_db_connection()

    try:
        case_number = request.form.get('case_number')
        view_role = request.form.get('view_role')
        remarks = request.form.get('remarks')
        user_name = request.form.get('user_name')
        assigned_to = request.form.get('assigned_to')
        cc = request.form.get('cc')
        name = request.form.get('name')

        # Validate required fields
        if not case_number or not user_name or not view_role or not remarks:
            return jsonify({"success": False, "message": "Missing required fields"}), 400

        # Check if remarks already exist
        processed_db_cursor.execute(
            "SELECT remarks FROM remarks WHERE case_id = %s AND assigned_by = %s AND assigned_to = %s",
            (case_number, user_name, assigned_to)
        )
        row = processed_db_cursor.fetchone()

        if row and row[0]:
            return jsonify({"success": True, "message": "Remarks already exist for this case_number, assigned_by, and assigned_to."}), 200

        image_path = None  # Default: No image
        if "image" in request.files:
            image_file = request.files["image"]
            if image_file and utils.allowed_file(image_file.filename):
                filename = secure_filename(f"{case_number}_{datetime.now().strftime('%Y%m%d%H%M%S')}.jpg")
                BASE_DIR = os.path.abspath(os.path.dirname(__file__))
                STATIC_FOLDER = os.path.abspath(os.path.join(BASE_DIR, "..", "static", "images"))

                os.makedirs(STATIC_FOLDER, exist_ok=True)
                file_path = os.path.join(STATIC_FOLDER, filename)

                image_file.save(file_path)

                image_path = f"static/images/{filename}"

        new_remark = {
            "message": remarks,
            "messaged_by": user_name,
            "messaged_to": assigned_to,
            "cc": cc,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "name": name,
            "image_path": image_path
        }

        new_remark_json = json.dumps([new_remark])
        insert_query = """
            INSERT INTO remarks (case_id, assigned_to, assigned_by, cc, remarks, image_path)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        processed_db_cursor.execute(
            insert_query,
            (case_number, assigned_to, user_name, cc, new_remark_json, image_path)
        )
        processed_db_conn.commit()

        notification_query = """
            INSERT INTO realtime_notifications (case_number, user_name, update_from, type)
            VALUES (%s, %s, %s, %s)
        """
        notification_cursor.execute(
            notification_query,
            (case_number, user_name, assigned_to, 'remark')
        )
        notification_conn.commit()

        response_message = "Remarks added successfully."
        return jsonify({
            "success": True,
            "message": response_message,
        }), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({"success": False, "message": "An unexpected error occurred. Please try again later."}), 500

    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        processed_db_cursor.close()
        notification_cursor.close()
        postgresql_pool.putconn(processed_db_conn)
        notification_pool.putconn(notification_conn)


def parse_timestamp(remarks):
    last_timestamp = utils.get_last_timestamp(remarks)
    temp = datetime.strptime(last_timestamp, configs.YMD_HMS) if last_timestamp else datetime.min
    return temp


# @app.route(configs.GET_REMARKS['ENDPOINT'], methods=[configs.GET_REMARKS['METHOD']])  # Changed to POST
# @limiter.limit(configs.LIMITER)
# @require_api_key
# @jwt_required()
# def get_remarks():
#     log_db_conn, log_db_cursor = get_log_db_connection()
#     processed_db_conn, processed_db_cursor = get_processed_db_connection()
#     try:
#         district_str = request.form.get('district')
#         from_date = request.form.get('fromDate')
#         to_date = request.form.get('toDate')
#         view_role = request.form.get('view_role', type=int)
#         police_station_str = request.form.get('police_station')
#         username = request.form.get('username')
#         name = request.form.get('name')
#
#         if username:
#             username = username.lower()
#
#         districts = district_str.split(",") if district_str else []
#         police_stations = police_station_str.split(",") if police_station_str else []
#
#         if not all([from_date, to_date, username]):
#             return jsonify({
#                 'status': False,
#                 'message': 'Missing required parameters',
#                 'data': None
#             }), 400
#
#         district_ids = []
#         if districts:
#             for district in districts:
#                 if district in configs.REVERSED_DISTRICTS_DICTIONARY:
#                     district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
#                 else:
#                     return jsonify({
#                         'status': False,
#                         'message': f"Invalid district name: {district}",
#                         'data': None
#                     }), 400
#
#         if view_role == 5 and district_ids:
#             district_condition = (
#                 f"AND district_id IN ({', '.join(map(str, district_ids))}) "
#                 f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
#             )
#         elif view_role in [3, 4]:
#             district_condition = f"AND district_id IN ({', '.join(map(str, district_ids))})"
#         else:
#             district_condition = ""
#
#         my_followups_query = f"""
#                 SELECT case_number, level3_case_nature, caller_name, caller_number,
#                        accepted_time AS created_time, police_station, district_id, time_id,
#                        description, first_arrival_time AS reached_time, response_time, assignedby_remarks AS assigned_by, remarks_status
#                 FROM response_time
#                 WHERE (level1_case_nature IN ('Crime Against Person', 'Crime Against Property') OR level3_case_nature IN ('Aerial Firing', 'Attempt to Illegal Possession of Land/ Premises'))
#                       {district_condition}
#                   AND district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
#                   AND police_station IS NOT NULL
#                   AND parent_id = 0
#                   AND assignedto_remarks = %s
#             """
#         my_followups_query = my_followups_query.format(district_condition=district_condition)
#
#         fp_cases = []
#         if username != 'ig.punjab':
#             processed_db_cursor.execute(my_followups_query, (username,))
#             fp_cases = processed_db_cursor.fetchall()
#
#         followups_needed_query = f"""
#                 SELECT case_number, level3_case_nature, caller_name, caller_number,
#                        accepted_time AS created_time, police_station, district_id, time_id,
#                        description, first_arrival_time AS reached_time, response_time, assignedto_remarks, remarks_status,remarks
#                 FROM response_time
#                 WHERE (level1_case_nature IN ('Crime Against Person', 'Crime Against Property') OR
#                     level3_case_nature IN ('Aerial Firing', 'Attempt to Illegal Possession of Land/ Premises'))
#                       {district_condition}
#                   AND district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
#                   AND police_station IS NOT NULL
#                   AND parent_id = 0
#                   AND  assignedby_remarks = %s
#             """
#         followups_needed_query = followups_needed_query.format(district_condition=district_condition)
#         processed_db_cursor.execute(followups_needed_query, (name,))
#         fp_needed_cases = processed_db_cursor.fetchall()
#
#         my_followups_list = [
#             {
#                 "case_number": case_number,
#                 "case_nature": level3_case_nature,
#                 "caller_name": caller_name,
#                 "assigned_time": created_time,
#                 "cli": caller_number,
#                 "police_station": police_station,
#                 "status": 'CompCa',
#                 "district": configs.DISTRICTS_DICTIONARY.get(int(district_id)),
#                 "time_id": datetime.fromtimestamp(int(time_id)).strftime(configs.YMD_HMS) if time_id else None,
#                 "description": description,
#                 "reached_time": datetime.fromtimestamp(int(reached_time)).strftime(
#                     configs.YMD_HMS) if reached_time else None,
#                 "response_time": f"{int(response_time // 60)}:{int(response_time % 60):02d}" if response_time else None,
#                 "assignedby": assigned_by,
#                 "priority_flag": 1 if time_id and (datetime.now() - datetime.fromtimestamp(
#                     int(time_id))).days < 3 and remarks_status == 'UnAnswered' else 0,
#                 "status_flag": 1 if remarks_status != 'UnAnswered' else 0
#             }
#             for (case_number, level3_case_nature, caller_name, caller_number,
#                  created_time, police_station, district_id, time_id,
#                  description, reached_time, response_time, assigned_by, remarks_status) in (fp_cases or [])
#         ]
#
#         current_time = datetime.now()
#
#         followups_neeeded_list = [
#             {
#                 "case_number": case_number,
#                 "case_nature": level3_case_nature,
#                 "caller_name": caller_name,
#                 "assigned_time": created_time,
#                 "cli": caller_number,
#                 "police_station": police_station,
#                 "status": 'CompCa',
#                 "district": configs.DISTRICTS_DICTIONARY.get(int(district_id)),
#                 "time_id": datetime.fromtimestamp(int(time_id)).strftime(configs.YMD_HMS) if time_id else None,
#                 "description": description,
#                 "reached_time": datetime.fromtimestamp(int(reached_time)).strftime(
#                     configs.YMD_HMS) if reached_time else None,
#                 "response_time": f"{int(response_time // 60)}:{int(response_time % 60):02d}" if response_time else None,
#                 "assignedto": utils.split_name(assigned_to),
#                 "priority_flag": 1 if time_id and (datetime.now() - datetime.fromtimestamp(
#                     int(time_id))).days < 3 and remarks_status == 'UnAnswered' else 0,
#                 "status_flag": 1 if remarks_status != 'UnAnswered' else 0,
#                 "is_notify": (
#                     1
#                     if (last_timestamp := utils.get_last_timestamp(remarks)) and
#                        (current_time - datetime.strptime(last_timestamp, configs.YMD_HMS)).total_seconds() < 30
#                     else 0
#                 )
#             }
#             for (case_number, level3_case_nature, caller_name, caller_number,
#                  created_time, police_station, district_id, time_id,
#                  description, reached_time, response_time, assigned_to, remarks_status, remarks) in
#             (fp_needed_cases or [])
#         ]
#
#         if my_followups_list or followups_neeeded_list:
#             response = {
#                 'status': True,
#                 'message': 'Data fetched successfully',
#                 'data': {
#                     'myfollow_ups': my_followups_list,
#                     'follow_ups_needed': followups_neeeded_list
#                 }
#             }
#         else:
#             response = {
#                 'status': True,
#                 'data': "No Data Found"
#             }
#
#         return jsonify(response), 200
#
#     except Exception as e:
#         utils.log_to_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc())
#         return jsonify({
#             'status': False,
#             'message': 'Internal server error',
#             'data': None
#         }), 500
#     finally:
#         log_db_cursor.close()
#         log_db_conn.close()
#         processed_db_cursor.close()
#         postgresql_pool.putconn(processed_db_conn)


@app.route(configs.GET_REMARKS['ENDPOINT'], methods=[configs.GET_REMARKS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def get_remarks():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    try:
        # get request parameters
        district_str = request.form.get('district')
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        view_role = request.form.get('view_role', type=int)
        police_station_str = request.form.get('police_station')
        username = request.form.get('username')
        name = request.form.get('name')

        if username:
            username = username.lower()

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        if not all([from_date, to_date, username]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        # converted district names to IDs
        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        # built district condition clause
        if view_role == 5 and district_ids:
            district_condition = (
                f"AND rt.district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND rt.police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            district_condition = f"AND rt.district_id IN ({', '.join(map(str, district_ids))})"
        else:
            district_condition = ""

        # base query joining response_time and remarks tables
        base_query = f"""
            SELECT 
                rt.case_number, 
                rt.level3_case_nature, 
                rt.caller_name, 
                rt.caller_number,
                rt.accepted_time AS created_time, 
                rt.police_station, 
                rt.district_id, 
                rt.time_id,
                rt.description, 
                rt.first_arrival_time AS reached_time, 
                rt.response_time, 
                r.assigned_by, 
                r.assigned_to, 
                r.cc, 
                r.remarks, 
                r.time_stamp,
                r.image_path
            FROM response_time rt
            JOIN remarks r ON rt.case_number = r.case_id
            WHERE 
                  rt.district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
                  {district_condition}
                  AND rt.police_station IS NOT NULL
                  AND rt.parent_id = 0
        """

        # query for "My Follow-ups": remarks assigned to current user
        my_followups_query = base_query + " AND r.assigned_to = %s"
        processed_db_cursor.execute(my_followups_query, (username,))
        my_followups_rows = processed_db_cursor.fetchall()

        # query for "Follow-ups Needed": remarks sent by current user
        followups_needed_query = base_query + " AND r.assigned_by = %s"
        processed_db_cursor.execute(followups_needed_query, (username,))
        followups_needed_rows = processed_db_cursor.fetchall()

        # query for "CC Follow-ups": cases where current user is in cc field
        cc_query = base_query + " AND r.cc = %s"
        processed_db_cursor.execute(cc_query, (username,))
        cc_followups_rows = processed_db_cursor.fetchall()

        my_followups_list = []
        for row in my_followups_rows:
            (case_number, level3_case_nature, caller_name, caller_number, created_time,
             police_station, district_id, time_id, description, reached_time, response_time,
             assigned_by, assigned_to, cc, remark_text, time_stamp,image_path) = row

            status_flag, priority_flag = utils.get_remark_flags(remark_text, time_id)

            my_followups_list.append({
                "case_number": case_number,
                "case_nature": level3_case_nature,
                "caller_name": caller_name,
                "assigned_time": created_time,
                "cli": caller_number,
                "police_station": police_station,
                "status": 'CompCa',
                "district": configs.DISTRICTS_DICTIONARY.get(int(district_id)) if district_id else None,
                "time_id": datetime.fromtimestamp(int(time_id)).strftime(configs.YMD_HMS) if time_id else None,
                "description": description,
                "reached_time": datetime.fromtimestamp(int(reached_time)).strftime(
                    configs.YMD_HMS) if reached_time else None,
                "response_time": f"{int(response_time // 60)}:{int(response_time % 60):02d}" if response_time else None,
                "assignedby": assigned_by,
                "priority_flag": priority_flag,
                "status_flag": status_flag,
                "last_timestamp": time_stamp,
                "image_url" : (os.getenv('BASE_URL') + image_path ) if image_path else None
            })

        current_time = datetime.now()
        followups_needed_list = []
        for row in followups_needed_rows:
            (case_number, level3_case_nature, caller_name, caller_number, created_time,
             police_station, district_id, time_id, description, reached_time, response_time,
             assigned_by, assigned_to, cc, remark_text, time_stamp,image_path) = row

            status_flag, priority_flag = utils.get_remark_flags(remark_text, time_id)

            followups_needed_list.append({
                "case_number": case_number,
                "case_nature": level3_case_nature,
                "caller_name": caller_name,
                "assigned_time": created_time,
                "cli": caller_number,
                "police_station": police_station,
                "status": 'CompCa',
                "district": configs.DISTRICTS_DICTIONARY.get(int(district_id)) if district_id else None,
                "time_id": datetime.fromtimestamp(int(time_id)).strftime(configs.YMD_HMS) if time_id else None,
                "description": description,
                "reached_time": datetime.fromtimestamp(int(reached_time)).strftime(
                    configs.YMD_HMS) if reached_time else None,
                "response_time": f"{int(response_time // 60)}:{int(response_time % 60):02d}" if response_time else None,
                "assignedto": assigned_to,  # utils.split_name(assigned_to) if assigned_to else None
                "priority_flag": priority_flag,
                "status_flag": status_flag,
                "is_notify": (
                    1
                    if (last_timestamp := utils.get_last_timestamp(remark_text)) and
                       (current_time - datetime.strptime(last_timestamp, configs.YMD_HMS)).total_seconds() < 30
                    else 0
                ),
                "last_timestamp": time_stamp,
                "image_url": (os.getenv('BASE_URL') + image_path ) if image_path else None
            })

        cc_followups_list = []
        for row in cc_followups_rows:
            (case_number, level3_case_nature, caller_name, caller_number, created_time,
             police_station, district_id, time_id, description, reached_time, response_time,
             assigned_by, assigned_to, cc, remark_text, time_stamp, image_path) = row

            status_flag, priority_flag = utils.get_remark_flags(remark_text, time_id)

            cc_followups_list.append({
                "case_number": case_number,
                "case_nature": level3_case_nature,
                "caller_name": caller_name,
                "assigned_time": created_time,
                "cli": caller_number,
                "police_station": police_station,
                "status": 'CompCa',
                "district": configs.DISTRICTS_DICTIONARY.get(int(district_id)) if district_id else None,
                "time_id": datetime.fromtimestamp(int(time_id)).strftime(configs.YMD_HMS) if time_id else None,
                "description": description,
                "reached_time": datetime.fromtimestamp(int(reached_time)).strftime(
                    configs.YMD_HMS) if reached_time else None,
                "response_time": f"{int(response_time // 60)}:{int(response_time % 60):02d}" if response_time else None,
                "assigned_by": assigned_by,
                "cc": cc,
                "priority_flag": priority_flag,
                "status_flag": status_flag,
                "last_timestamp": time_stamp,
                "image_url" : (os.getenv('BASE_URL') + image_path ) if image_path else None
            })

        # sorted lists by timestamp (newest first)
        my_followups_list.sort(key=lambda x: x["last_timestamp"], reverse=True)
        followups_needed_list.sort(key=lambda x: x["last_timestamp"], reverse=True)
        cc_followups_list.sort(key=lambda x: x["last_timestamp"], reverse=True)

        if my_followups_list or followups_needed_list or cc_followups_list:
            response = {
                'status': True,
                'message': 'Data fetched successfully',
                'data': {
                    'myfollow_ups': my_followups_list,
                    'follow_ups_needed': followups_needed_list,
                    'cc_follow_ups': cc_followups_list
                }
            }
        else:
            response = {
                'status': True,
                'data': "No Data Found"
            }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': 'An unexpected error occurred. Please try again later.',
            'data': None
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route('/static/images/<path:filename>',methods = ['GET'])
def serve_static(filename):
    images_directory = os.path.join(os.getenv('STATIC_FOLDER'), 'images')
    return send_from_directory(images_directory, filename),200


@app.route(configs.UPDATE_REMARKS['ENDPOINT'], methods=[configs.UPDATE_REMARKS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def update_remarks():
    try:
        # Retrieve parameters from the request
        case_number = request.form.get('case_number')
        view_role = request.form.get('view_role')
        new_remarks = request.form.get('remarks')
        user_name = request.form.get('username')
        cc = request.form.get('cc')
        name = request.form.get('name')
        reciever = request.form.get('assigned_to')
        image = request.files.get('image')

        # Validate required fields
        if not case_number or not user_name or not view_role or not new_remarks:
            return jsonify({
                "success": False,
                "message": "Missing required fields"
            }), 400

        image_path = None
        if image and utils.allowed_file(image.filename):
            filename = secure_filename(f"{case_number}_{datetime.now().strftime('%Y%m%d%H%M%S')}.jpg")
            BASE_DIR = os.path.abspath(os.path.dirname(__file__))
            STATIC_FOLDER = os.path.abspath(os.path.join(BASE_DIR, "..", "static", "images"))

            os.makedirs(STATIC_FOLDER, exist_ok=True)
            file_path = os.path.join(STATIC_FOLDER, filename)

            image.save(file_path)  # Save image

            # Store only relative path
            image_path = f"static/images/{filename}"

            # Build the new remark object in the same structured format
        appended_remark = {
            "message": new_remarks,
            "messaged_by": user_name,
            "messaged_to": reciever,
            "cc": cc if cc else "",
            "name": name,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "image_url": image_path
        }

        # Connect to the processed and log databases
        processed_db_conn, processed_db_cursor = get_processed_db_connection()
        log_db_conn, log_db_cursor = get_log_pg_db_connection()
        notification_conn, notification_cursor = get_notification_db_connection()

        # Handle Exceptional Case for ig.punjab
        if user_name.lower() == "ig.punjab":
            processed_db_cursor.execute(
                "SELECT remarks FROM remarks WHERE case_id = %s AND assigned_to = %s AND assigned_by = %s",
                (case_number, reciever, user_name)
            )
        else:
            processed_db_cursor.execute(
                "SELECT remarks FROM remarks WHERE case_id = %s AND assigned_to = %s AND assigned_by = %s",
                (case_number, user_name, reciever)
            )
        row = processed_db_cursor.fetchone()

        if row:
            existing_remarks_json = row[0]
            if existing_remarks_json:
                try:
                    existing_remarks = json.loads(existing_remarks_json)
                    if isinstance(existing_remarks, list):
                        existing_remarks.append(appended_remark)
                    else:
                        existing_remarks = [existing_remarks, appended_remark]
                except json.JSONDecodeError:
                    existing_remarks = [appended_remark]
            else:
                existing_remarks = [appended_remark]

            updated_remarks_json = json.dumps(existing_remarks)

            # Update the remarks record in the database
            if user_name.lower() == "ig.punjab":
                processed_db_cursor.execute(
                    "UPDATE remarks SET remarks = %s WHERE case_id = %s AND assigned_to = %s AND assigned_by = %s",
                    (updated_remarks_json, case_number, reciever, user_name)
                )
            else:
                processed_db_cursor.execute(
                    "UPDATE remarks SET remarks = %s WHERE case_id = %s AND assigned_to = %s AND assigned_by = %s",
                    (updated_remarks_json, case_number, user_name, reciever)
                )
            processed_db_conn.commit()

            notification_query = """
                               INSERT INTO realtime_notifications (case_number, user_name, update_from, type)
                               VALUES (%s, %s, %s, %s)
                           """
            notification_cursor.execute(
                notification_query,
                (case_number, user_name, reciever, 'feedback')
            )

            notification_conn.commit()

            response_message = "Remarks updated successfully."

        else:
            response_message = "No record found for the provided case_number, assigned_by, and assigned_to."

        return jsonify({
            "success": True,
            "message": response_message
        }), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500

    finally:
        # Clean up database connections and cursors
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn) # Properly return to the pool without removing it

        processed_db_cursor.close()
        notification_cursor.close()
        postgresql_pool.putconn(processed_db_conn)
        notification_pool.putconn(notification_conn)


@app.route(configs.GET_NOTIFICATIONS['ENDPOINT'], methods=[configs.GET_NOTIFICATIONS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def get_notifications():
    notification_conn, notification_cursor = None, None
    log_db_conn, log_db_cursor = None, None
    try:
        # Connect to the databases
        notification_conn, notification_cursor = get_notification_db_connection()
        log_db_conn, log_db_cursor = get_log_pg_db_connection()

        view_role = request.form.get('view_role', type=int)
        user_name = request.form.get('username')

        # Validated required fields
        if not user_name or not view_role:
            return jsonify({
                "success": False,
                "message": "Missing required fields"
            }), 400

        notification_cursor.execute(
            "SELECT user_name, case_number, update_from, type FROM realtime_notifications WHERE update_from = %s ",
            (user_name,)
        )
        rows = notification_cursor.fetchall()

        result = [
            {
                "user_name": row[2],
                "case_number": row[1],
                "assigned_by": row[0],
                "type": row[3]
            }
            for row in rows
        ]

        # Delete the fetched notifications from the database
        notification_cursor.execute(
            "DELETE FROM realtime_notifications WHERE update_from = %s",
            (user_name,)
        )
        notification_conn.commit()

        response = {
            "status": True,
            "message": "Notifications fetched successfully",
            "data": result
        }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500

    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)  # Properly return to the pool without removing it

        if notification_cursor:
            notification_cursor.close()
        if notification_conn:
            notification_pool.putconn(notification_conn, close=False)


@app.route(configs.CM_DIST_RESPONSE_TIME['ENDPOINT'], methods=[configs.CM_DIST_RESPONSE_TIME['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def dist_response_time():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    try:
        district_str = request.form.get('district')
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        view_role = request.form.get('view_role', type=int)
        police_station_str = request.form.get('police_station')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        if not all([view_role, from_date, to_date]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        if view_role == 5 and district_ids:
            district_condition = (
                f"  AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            district_condition = f" AND district_id IN ({', '.join(map(str, district_ids))})"
        else:
            district_condition = ""

        district_query = """SELECT district_id,avg(response_time) FROM response_time 
                        WHERE district_id NOT IN ('0','41','42','43','44','45','46')
                        AND district_id IS NOT NULL 
                        AND date BETWEEN %s AND %s 
                        AND parent_id = 0
                        AND response_time is NOT NULL AND response_time > 0
                        {district_condition}
                        GROUP BY district_id
                        """
        district_query = district_query.format(district_condition=district_condition)

        processed_db_cursor.execute(district_query, (from_date, to_date))

        dist_results = processed_db_cursor.fetchall()

        districts_response_time = {
            configs.DISTRICTS_DICTIONARY[int(district[0])]: f"{int(district[1] // 60)}:{int(district[1] % 60):02d}"
            for district in dist_results
        }

        response = {
            "status": True,
            "message": "Response Time fetched successfully",
            "data": districts_response_time
        }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.CM_PS_RESPONSE_TIME['ENDPOINT'], methods=[configs.CM_PS_RESPONSE_TIME['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def cm_ps_responsetime():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    try:
        district_str = request.form.get('district')
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        view_role = request.form.get('view_role', type=int)
        police_station_str = request.form.get('police_station')

        if not all([district_str, from_date, to_date]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        if district_str:
            district_id = configs.REVERSED_DISTRICTS_DICTIONARY[district_str]

        if view_role == 5 and district_id:
            district_condition = (
                f"  AND district_id IN ({district_id}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        else:
            district_condition = ""

        ps_responsetime_query = """
                        SELECT police_station, avg(response_time)
                        FROM response_time
                        WHERE district_id NOT IN ('0','41','42','43','44','45','46')
                        AND district_id IS NOT NULL
                        AND date BETWEEN %s AND %s
                        AND parent_id = 0
                        AND response_time IS NOT NULL AND response_time > 0
                        AND district_id = %s
                        {district_condition}
                        AND police_station IS NOT NULL
                        GROUP BY police_station
                    """
        ps_responsetime_query = ps_responsetime_query.format(district_condition=district_condition)

        processed_db_cursor.execute(ps_responsetime_query, (from_date, to_date, district_id))

        ps_results = processed_db_cursor.fetchall()

        ps_response_time = {
            ps[0]: f"{int(ps[1] // 60)}:{int(ps[1] % 60):02d}"
            for ps in ps_results
        }

        response = {
            "status": True,
            "message": "Response Time fetched successfully",
            "data": ps_response_time
        }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.DISTRICT_FIR_DATA['ENDPOINT'], methods=[configs.DISTRICT_FIR_DATA['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def district_fir_stats():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()

    try:
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        view_role = request.form.get('view_role', type=int)
        district_str = request.form.get('district')
        police_station_str = request.form.get('police_station')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        if not all([view_role, from_date, to_date]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        if view_role == 5 and district_ids:
            district_condition = (
                f"  AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            district_condition = f" AND district_id IN ({', '.join(map(str, district_ids))})"
        else:
            district_condition = ""

        fir_stats_query = """
                SELECT 
                    district,
                    SUM(fir_count)
                FROM 
                    fir_data
                WHERE 
                    date BETWEEN %s AND %s
                    AND district_id is not NULL
                    AND district_id NOT IN ('0','41','42','43','44','45','46')
                    {district_condition}
                GROUP BY 
                    district;
                 """
        fir_stats_query = fir_stats_query.format(district_condition=district_condition)

        processed_db_cursor.execute(fir_stats_query, (from_date, to_date))
        district_fir_data = processed_db_cursor.fetchall()

        registered_fir_response = {
            district[0]: district[1]
            for district in district_fir_data
        }

        cases_query = """
                SELECT
                    district_id,
                    count(*)
                FROM
                    response_time
                WHERE
                    date BETWEEN %s AND %s
                    AND district_id is not Null 
                    AND parent_id = 0
                    AND district_id NOT IN ('0','41','42','43','44','45','46')
                    {district_condition}
                GROUP BY 
                    district_id
        """

        cases_query = cases_query.format(district_condition=district_condition)

        processed_db_cursor.execute(cases_query, (from_date, to_date))
        cases = processed_db_cursor.fetchall()

        cases_response = {
            configs.DISTRICTS_DICTIONARY[int(case[0])]: case[1]
            for case in cases
        }

        unregistered_fir = {}

        for district, case_count in cases_response.items():
            if district not in unregistered_fir:
                fir_count = registered_fir_response.get(district, 0)
                unregistered_fir[district] = case_count - fir_count

        response = {
            "status": True,
            "message": "District-wise FIRs Registered and Not Registered fetched successfully",
            "data": {
                'fir_registered': registered_fir_response,
                'fir_unregistered': unregistered_fir
            }
        }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it

        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.PS_FIR_DATA['ENDPOINT'], methods=[configs.PS_FIR_DATA['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def ps_fir_stats():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()

    try:
        district_str = request.form.get('district')
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        view_role = request.form.get('view_role', type=int)
        police_station_str = request.form.get('police_station')

        if not all([district_str, from_date, to_date]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        if district_str:
            district_id = configs.REVERSED_DISTRICTS_DICTIONARY[district_str]

        police_stations = police_station_str.split(",") if police_station_str else []

        if view_role == 5:
            district_condition = f" AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
        else:
            district_condition = ""

        fir_stats_query = """
                SELECT 
                    police_station,
                    SUM(fir_count)
                FROM 
                    fir_data
                WHERE 
                    date BETWEEN %s AND %s
                    AND district_id is not NULL
                    AND district_id = %s
                    {district_condition}
                GROUP BY 
                    police_station;
                 """
        fir_stats_query = fir_stats_query.format(district_condition=district_condition)

        processed_db_cursor.execute(fir_stats_query, (from_date, to_date, district_id))
        ps_fir_data = processed_db_cursor.fetchall()

        ps_fir_response = {
            ps[0]: ps[1]
            for ps in ps_fir_data
        }

        cases_query = """
                        SELECT
                            police_station,
                            count(*)
                        FROM
                            response_time
                        WHERE
                            date BETWEEN %s AND %s
                            AND district_id = %s
                            AND parent_id = 0
                            AND district_id NOT IN ('0','41','42','43','44','45','46')
                            {district_condition}
                        GROUP BY 
                            police_station
                """

        cases_query = cases_query.format(district_condition=district_condition)

        processed_db_cursor.execute(cases_query, (from_date, to_date, district_id))
        cases = processed_db_cursor.fetchall()

        cases_response = {
            case[0]: case[1]
            for case in cases
        }

        unregistered_fir = {}

        for ps, case_count in cases_response.items():
            if ps not in unregistered_fir:
                fir_count = ps_fir_response.get(ps, 0)
                unregistered_fir[ps] = case_count - fir_count

        response = {
            "status": True,
            "message": "Ps-wise Fir registered and Unregistered Data fetched successfully",
            "data": {
                'fir_registered': ps_fir_response,
                'fir_unregistered': unregistered_fir
            }
        }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.CONFERENCE_CALL_STATS['ENDPOINT'], methods=[configs.CONFERENCE_CALL_STATS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def conference_call_stats():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()

    try:
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        view_role = request.form.get('view_role', type=int)
        district_str = request.form.get('district')
        police_station_str = request.form.get('police_station')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        if not all([view_role, from_date, to_date]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        if view_role == 5 and district_ids:
            district_condition = (
                f"  AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            district_condition = f" AND district_id IN ({', '.join(map(str, district_ids))})"
        else:
            district_condition = ""

        dist_conf_query = """
                SELECT 
                    district_id,
                    SUM(CASE WHEN field3 = 'Successful Conference call' THEN 1 ELSE 0 END) AS successful_calls,
                    SUM(CASE WHEN field3 IN ('FO did not attend the call','Number Powered Off') THEN 1 ELSE 0 END) AS Unsuccessful_calls
                FROM 
                    response_time
                WHERE 
                    date BETWEEN %s AND %s
                    AND field3 IS NOT NULL
                    AND district_id is not NULL
                    AND district_id NOT IN ('0','41','42','43','44','45','46')
                    AND parent_id = 0
                    {district_condition}
                GROUP BY 
                    district_id;
                 """

        dist_conf_query = dist_conf_query.format(district_condition=district_condition)

        processed_db_cursor.execute(dist_conf_query, (from_date, to_date))
        dist_conf_results = processed_db_cursor.fetchall()

        district_response = {
            configs.DISTRICTS_DICTIONARY[int(district[0])]: {
                'successful': district[1],
                'unsuccessful': district[2]
            }
            for district in dist_conf_results
        }

        return jsonify(district_response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.ALERT_RESPONSETIME['ENDPOINT'], methods=[configs.ALERT_RESPONSETIME['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def response_time_alerts():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    try:
        view_role = request.form.get('view_role', type=int)
        district_str = request.form.get('district')
        police_station_str = request.form.get('police_station')
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        if not all([view_role]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        if view_role == 5 and district_ids:
            district_condition = (
                f"  AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            district_condition = f" AND district_id IN ({', '.join(map(str, district_ids))})"
        else:
            district_condition = ""

        alerts_count_query = """
                    SELECT district_id, COUNT(case_number) AS case_count
                    FROM response_time
                    Where date between %s AND %s
                    AND response_time > 2100 
                    AND parent_id = 0
                    AND district_id IS NOT NULL
                    AND district_id NOT IN ('0','41','42','43','44','45','46')
                    AND level3_case_nature NOT IN ('Other Help')
                    AND level2_case_nature in ('Robbery/Snatching', 'Burglary', 'Dacoity', 'Sexual Assault', 'Kiddnapping / Abduction', 'Murder', 'Terrorist Act')
                    {district_condition}
                    GROUP BY district_id
        """
        alerts_count_query = alerts_count_query.format(district_condition=district_condition)

        processed_db_cursor.execute(alerts_count_query, (from_date, to_date))
        alerts = processed_db_cursor.fetchall()

        alerts_count = []
        for i in alerts:
            alerts_count.append({configs.DISTRICTS_DICTIONARY[int(i[0])]: i[1]})

        alerts_casenumbers = """
                    SELECT district_id, STRING_AGG(
                           CONCAT('Case Number: ', case_number, 
                                  ', District: ', district_id, 
                                  ', Case Nature: ', level3_case_nature, 
                                  ', Police Station: ', police_station
                           ), 
                           ' ; ' -- Separator for concatenation
                       ) AS details
                    FROM response_time
                    WHERE date BETWEEN %s AND %s
                      AND response_time > 2100
                      AND parent_id = 0
                      AND district_id IS NOT NULL
                      AND district_id NOT IN ('0','41','42','43','44','45','46')
                      AND level3_case_nature NOT IN ('Other Help')
                      AND level2_case_nature in ('Robbery/Snatching', 'Burglary', 'Dacoity', 'Sexual Assault', 'Kiddnapping / Abduction', 'Murder', 'Terrorist Act')
                      {district_condition}
                    GROUP BY district_id;
        """

        alerts_casenumbers = alerts_casenumbers.format(district_condition=district_condition)

        processed_db_cursor.execute(alerts_casenumbers, (from_date, to_date))
        alerts = processed_db_cursor.fetchall()

        alerts_cases = []
        for i in alerts:
            alerts_cases.append({configs.DISTRICTS_DICTIONARY[int(i[0])]: i[1].split(";")})

        if view_role == 5 and district_ids:
            additional_condition = (
                f"  AND district IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            additional_condition = f" AND district IN ({', '.join(map(str, district_ids))})"
        else:
            additional_condition = ""

        db_conn = db_config.get_db_connection()
        db_cursor = db_conn.cursor()

        current_date = datetime.now()

        current_date_str = current_date.strftime("%Y-%m-%d")

        db_cursor.execute(f"""
                    SELECT district, GROUP_CONCAT(
                        CONCAT('Case Number: ', case_number, 
                               ', District: ', district, 
                               ', Case Nature: ', case_nature,
                               ', Police Station: ',police_station)
                        SEPARATOR ' ; '
                    ) AS details 
                    FROM pred_pol_crimes_hotspot 
                    WHERE case_number IS NOT null
                    AND date = %s
                    AND case_nature in ('Highway/Road/Street Robbery', 'Shop Robbery', 'Patrol Pump Robbery', 'Any Other Robbery', 
                    'Snatching/Jhapatta', 'House Robbery', 'Cattle Robbery', 'Robbery with Murder', 'Bank/Money Exchange/ ATM Robbery', 
                    'Jewellery Shop Robbery','Motorcycle Snatching', 'Bank Burglary', 'House Burglary', 'Shop Burglary', 'Other Burglary', 
                    'Dacoity with Murder', 'House Dacoity', 'Highway/Road/Street Dacoity', 'Cattle Dacoity', 'Shop Dacoity', 'Any Other Dacoity', 
                    'Patrol Pump Dacoity', 'Jewellery Shop Dacoity', 'Sexual Assault/ Harrasment To Women', 'Rape', 'Child Abuse / Molestation', 
                    'Male Kidnapping/ Abduction', 'Female Kidnapping/ Abduction', 'Attempt to Kidnap / Abduct', 'Child Kidnapping', 
                    'Kidnapping for Ransom','Illegal detention of a person', 'Attempt to Murder', 'Murder', 'Firing on Police', 
                    'Suicidal Attack/ Bomb Blast/ Terrorist Attack')
                    {additional_condition}
                    Group By district
        """, (current_date.strftime('%d-%m-%Y'),))
        re_occurrences_cases = db_cursor.fetchall()

        crime_occurence = []
        for row in re_occurrences_cases:
            # Convert the row into a list
            row = list(row)

            # Decode the district value if necessary
            row[0] = row[0].decode('utf-8') if isinstance(row[0], bytearray) else row[0]

            # Decode the details string if necessary
            row[1] = row[1].decode('utf-8') if isinstance(row[1], bytes) else row[1]

            # Split the details string on ' ; ' since that's our separator in the query
            details_list = row[1].split(" ; ")

            # Append the result using the district dictionary for mapping
            crime_occurence.append({configs.DISTRICTS_DICTIONARY[int(row[0])]: details_list})

        response = {
            "status": True,
            "message": "Response Time fetched successfully",
            "data": {
                "counts": alerts_count,
                "cases": alerts_cases,
                "crime_reoccurences": crime_occurence
            }
        }
        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.VEHICLE_LOCATIONS['ENDPOINT'], methods=[configs.VEHICLE_LOCATIONS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def police_vehicle_locations():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    try:
        vehicle_status = request.form.get('status')
        district_str = request.form.get('district')
        police_station_str = request.form.get('police_station')
        view_role = request.form.get('view_role', type=int)

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        if not all([view_role, vehicle_status]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        district_condition = ""
        if (view_role == 3 or view_role == 4) and districts:
            district_condition = f"AND district IN ({', '.join(repr(district) for district in districts)}) "
        elif view_role == 5 and districts:
            district_condition = (
                f"AND district IN ({', '.join(repr(district) for district in districts)}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )

        status_dict = {
            'online': "Free",
            'offline': "Idle",
            "busy": "Busy"
        }
        if vehicle_status.lower() == 'total':
            query = """ SELECT 
                            name , police_station , latitude , longitude, 
                            registration_no, district , unit_type 
                        FROM 
                            police_mv_locations 
                        WHERE 
                            1=1
                            {district_condition}
                    """
            params = ()

        else:
            query = """ SELECT 
                            name , police_station , latitude , longitude, 
                            registration_no, district , unit_type 
                        FROM 
                            police_mv_locations 
                        WHERE 
                            status = %s
                            {district_condition}
                    """
            params = (status_dict[vehicle_status],)

        query = query.format(district_condition=district_condition)

        with processed_db_conn.cursor(cursor_factory=RealDictCursor) as processed_db_cursor:
            processed_db_cursor.execute(query, params)
            records = processed_db_cursor.fetchall()

        data = [dict(row) for row in records]

        processed_db_conn.close()

        return jsonify(
            {"status": "success", "data": data, "message": "Police Vehicles Data fetched successfully", }), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.VWPS_STATS['ENDPOINT'], methods=[configs.VWPS_STATS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def vwps_stats():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    db_conn = db_config.get_vwps_db_connection()
    db_cursor = db_conn.cursor()
    try:
        district_str = request.form.get('district')
        police_station_str = request.form.get('police_station')
        view_role = request.form.get('view_role', type=int)
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        category = request.form.get('category')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []
        category = category.split(",") if category else []

        if not all([district_str, police_station_str, from_date, to_date]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        if view_role == 5 and district_ids:
            district_condition = (
                f"  AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND pucar_police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            district_condition = f" AND district_id IN ({', '.join(map(str, district_ids))})"
        else:
            district_condition = ""

        categories = f""" AND level3_case_nature IN ({', '.join(f"'{category.strip()}'" for category in category)}) """ if category else """
         AND level3_case_nature IN ('Female Kidnapping/ Abduction', 'Sexual Assault/ Harassment To Women', 'Domestic Violence', 'Rape', 
        'Child Abuse / Molestation', 'Prostitution/ Brothel House' ,'Attempt to Kidnap / Abduct', 'Murder', 'Kidnapping for Ransom' ,
        'Attempt to Murder' , 'Acid Throwing', 'Other Assault', 'Physical Threats / Harrasment', 'Hurt / Injuries', 'Missing Person reported', 
        'Missing Person Found', 'Attempt to Suicide', 'Suicide', 'Prostitution/ Brothel House')
        """

        from_date_obj = datetime.strptime(from_date, '%Y-%m-%d')
        to_date_obj = datetime.strptime(to_date, '%Y-%m-%d')

        current_date_str = from_date_obj.strftime('%Y-%m-%d 00:00:00')
        to_date_str = to_date_obj.strftime('%Y-%m-%d 23:59:59')

        vwps_query = f"""
               SELECT
                    COUNT(*) AS total_vwps,
                    SUM(CASE WHEN final_status_id = 8 THEN 1 ELSE 0 END +
                        CASE WHEN final_status_id = 1 THEN 1 ELSE 0 END +
                        CASE WHEN final_status_id = 7 THEN 1 ELSE 0 END) AS under_inquiry_vwps,
                    COUNT(CASE WHEN final_status_id = 7 THEN 1 ELSE NULL END) AS escalated_vwps,
                    SUM(CASE WHEN final_status_id = 2 THEN 1 ELSE 0 END +
                        CASE WHEN final_status_id = 3 THEN 1 ELSE 0 END +
                        CASE WHEN final_status_id IN (5, 9, 10, 11) THEN 1 ELSE 0 END) AS fir_vwps,
                    SUM(CASE WHEN final_status_id IN (5, 9, 10, 11) THEN 1 ELSE 0 END) AS challan_vwps,
                    SUM(CASE WHEN final_status_id = 6 THEN 1 ELSE 0 END) AS resolved_vwps
                FROM case_final_status
                WHERE created_at BETWEEN %s AND %s
                    AND district_id IS NOT NULL
                    {district_condition}
                """
        db_cursor.execute(vwps_query, (current_date_str, to_date_str))
        values = db_cursor.fetchone()

        (received_vwps_cases, under_inquiry_vwps_cases, escalated_vwps_cases,
         fir_registered_vwps_cases, challan_submitted_vwps_cases, resolved_vwps_cases) = [
            str(val).encode('utf-8').decode('utf-8') if isinstance(val, Decimal) else val
            for val in values
        ]

        cases_query = f"""
                    SELECT pucar_case_number, level3_case_nature, pucar_caller_name, pucar_cli,
                        pucar_police_station, final_status_remarks, pucar_cro_comments, pucar_district, created_at
                    FROM case_final_status
                    WHERE created_at BETWEEN %s AND %s
                    {district_condition}
                """
        db_cursor.execute(cases_query, (current_date_str, to_date_str))
        cases = db_cursor.fetchall()

        cases_list = [
            {
                "case_number": case_number,
                "case_nature": level3_case_nature,
                "caller_name": caller_name,
                "cli": caller_number,
                "police_station": police_station,
                "district": district,
                "description": description,
                "created_at": created_at,
                "status": final_status_remarks
            }
            for (case_number, level3_case_nature, caller_name, caller_number,
                 police_station, final_status_remarks, description, district, created_at) in cases
        ]

        if districts:
            district_ids = [repr(district) for district in district_ids]  # Safely quote district names
            district_condition = f" AND district_id IN ({', '.join(district_ids)})"

        # Build police station condition
        police_station_condition = ""
        if police_stations:
            police_station_names = [repr(ps) for ps in police_stations]  # Safely quote police station names
            police_station_condition = f" AND pucar_police_station IN ({', '.join(police_station_names)})"

        crime_hotspots_query = f"""
            Select district_id ,pucar_police_station, pucar_level2_case_nature , pucar_lat, pucar_long
            FROM case_final_status
            WHERE pucar_lat is NOT NULL
            AND pucar_long is NOT NULL
            AND pucar_lat != ''    
            AND pucar_long != ''
            {categories} 
            {district_condition}
            """

        db_cursor.execute(crime_hotspots_query, )
        hotspot_results = db_cursor.fetchall()

        # Initialize an empty list to store the coordinates
        coordinates = []

        # Loop through the fetched results
        for result in hotspot_results:
            # Extract the latitude and longitude values from the result
            reached_lat = result[3]
            reached_long = result[4]

            # Append the coordinates as a list to the coordinates list
            coordinates.append([float(reached_lat), float(reached_long)])

        response = {"status": "success",
                    "data": {
                        'stats': {
                            'recieved_cases': received_vwps_cases,
                            'under_inquiry_cases': under_inquiry_vwps_cases,
                            'escalated_cases': escalated_vwps_cases,
                            'fir_registered': fir_registered_vwps_cases,
                            'vwps_challan_count': challan_submitted_vwps_cases,
                            'resolved_cases': resolved_vwps_cases
                        },
                        'cases': cases_list,
                        'hotspot_coordinates': utils.filter_lat_longs(coordinates, district_str)
                    },
                    "message": "VWPS STATS AND CASES fetched successfully", }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)  # Properly return to the pool without removing it


@app.route(configs.VCCS_STATS['ENDPOINT'], methods=[configs.VCCS_STATS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def vccs_stats():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    db_conn = db_config.get_vccs_db_connection()
    db_cursor = db_conn.cursor()
    try:
        district_str = request.form.get('district')
        police_station_str = request.form.get('police_station')
        view_role = request.form.get('view_role', type=int)
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        category = request.form.get('category')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []
        category = category.split(",") if category else []

        if not all([district_str, from_date, to_date]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        if view_role == 5 and district_ids:
            district_condition = (
                f"  AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND pucar_police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            district_condition = f" AND district_id IN ({', '.join(map(str, district_ids))})"
        else:
            district_condition = ""

        categories = f""" AND level3_case_nature IN ({', '.join(f"'{category.strip()}'" for category in category)}) """ if category else """
         AND level3_case_nature IN ('Child Abuse / Molestation', 'Child Lost/ Missing', 'Child Kidnapping', 'Child Found', 'Child Labor' , 
         'Kidnapping for Ransom', 'Attempt to Kidnap / Abduct', 'Murder') 
        """

        from_date_obj = datetime.strptime(from_date, '%Y-%m-%d')
        to_date_obj = datetime.strptime(to_date, '%Y-%m-%d')

        current_date_str = from_date_obj.strftime('%Y-%m-%d 00:00:00')
        to_date_str = to_date_obj.strftime('%Y-%m-%d 23:59:59')

        vccs_query = f"""
                   SELECT
                    COUNT(*) AS total_vccs,
                    SUM(CASE WHEN final_status_id = 8 THEN 1 ELSE 0 END +
                        CASE WHEN created_at >= %s AND created_at <= %s AND final_status_id = 12 
                        AND (handed_over_to IN (1, 3, 4, 5)) THEN 1 ELSE 0 END) AS under_inquiry_vccs,
                    COUNT(CASE WHEN final_status_id = 7 THEN 1 ELSE NULL END) AS escalated_vccs,
                    COUNT(CASE WHEN is_fir_registered = 1 OR is_challan_submitted = 1 THEN 1 ELSE NULL END) AS fir_vccs,
                    COUNT(CASE WHEN is_challan_submitted = 1 THEN 1 ELSE NULL END) AS challan_vccs,
                    SUM(CASE WHEN final_status_id = 6 THEN 1 ELSE 0 END +
                        CASE WHEN final_status_id = 12 AND 
                        handed_over_to = 2 AND created_at >= %s AND created_at <= %s 
                        THEN 1 ELSE 0 END) AS resolved_vccs
                    FROM case_final_status
                    WHERE created_at BETWEEN %s AND %s
                    AND district_id IS NOT NULL
                    {district_condition}
                """
        db_cursor.execute(vccs_query,
                          (current_date_str, to_date_str, current_date_str, to_date_str, current_date_str, to_date_str))
        values = db_cursor.fetchone()

        (received_vccs_cases, under_inquiry_vccs_cases, escalated_vccs_cases,
         fir_registered_vccs_cases, challan_submitted_vccs_cases, resolved_vccs_cases) = [
            str(val).encode('utf-8').decode('utf-8') if isinstance(val, Decimal) else val
            for val in values
        ]

        cases_query = f"""
                    SELECT pucar_case_number, level3_case_nature, pucar_caller_name, pucar_cli,
                        pucar_police_station, final_status_remarks, pucar_cro_comments, pucar_district, created_at
                    FROM case_final_status
                    WHERE created_at BETWEEN %s AND %s
                    {district_condition}
                """
        db_cursor.execute(cases_query, (current_date_str, to_date_str))
        cases = db_cursor.fetchall()

        cases_list = [
            {
                "case_number": case_number,
                "case_nature": level3_case_nature,
                "caller_name": caller_name,
                "cli": caller_number,
                "police_station": police_station,
                "district": district,
                "description": description,
                "created_at": created_at,
                "status": final_status_remarks
            }
            for (case_number, level3_case_nature, caller_name, caller_number,
                 police_station, final_status_remarks, description, district, created_at) in cases
        ]

        if districts:
            district_ids = [repr(district) for district in district_ids]  # Safely quote district names
            district_condition = f" AND district_id IN ({', '.join(district_ids)})"

        # Build police station condition
        police_station_condition = ""
        if police_stations:
            police_station_names = [repr(ps) for ps in police_stations]  # Safely quote police station names
            police_station_condition = f" AND pucar_police_station IN ({', '.join(police_station_names)})"

        crime_hotspots_query = f"""
            Select district_id ,pucar_police_station, pucar_level2_case_nature , pucar_lat, pucar_long
            FROM case_final_status
            WHERE pucar_lat is NOT NULL
            AND pucar_long is NOT NULL
            AND pucar_lat != ''    
            AND pucar_long != ''
            {categories}
            {district_condition}
            """

        db_cursor.execute(crime_hotspots_query, )
        hotspot_results = db_cursor.fetchall()

        # Initialize an empty list to store the coordinates
        coordinates = []

        # Loop through the fetched results
        for result in hotspot_results:
            # Extract the latitude and longitude values from the result
            reached_lat = result[3]
            reached_long = result[4]

            # Append the coordinates as a list to the coordinates list
            coordinates.append([float(reached_lat), float(reached_long)])

        response = {"status": "success",
                    "data": {
                        'stats': {
                            'recieved_cases': received_vccs_cases,
                            'under_inquiry_cases': under_inquiry_vccs_cases,
                            'escalated_cases': escalated_vccs_cases,
                            'fir_registered': fir_registered_vccs_cases,
                            'vwps_challan_count': challan_submitted_vccs_cases,
                            'resolved_cases': resolved_vccs_cases
                        },
                        'cases': cases_list,
                        'hotspot_coordinates': utils.filter_lat_longs(coordinates, district_str)
                    },
                    "message": "VCCS STATS AND CASES fetched successfully", }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)  # Properly return to the pool without removing it


@app.route(configs.VCM_STATS['ENDPOINT'], methods=[configs.VCM_STATS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def vcm_stats():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    db_conn = db_config.get_vcm_db_connection()
    db_cursor = db_conn.cursor()
    try:
        district_str = request.form.get('district')
        police_station_str = request.form.get('police_station')
        view_role = request.form.get('view_role', type=int)
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        if not all([district_str, police_station_str, from_date, to_date]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        if view_role == 5 and district_ids:
            district_condition = (
                f"  AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND pucar_police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            district_condition = f" AND district_id IN ({', '.join(map(str, district_ids))})"
        else:
            district_condition = ""

        from_date_obj = datetime.strptime(from_date, '%Y-%m-%d')
        to_date_obj = datetime.strptime(to_date, '%Y-%m-%d')

        current_date_str = from_date_obj.strftime('%Y-%m-%d 00:00:00')
        to_date_str = to_date_obj.strftime('%Y-%m-%d 23:59:59')

        vcm_query = f"""
                    SELECT
                    COUNT(*) AS total_vcm,
                        SUM(CASE WHEN final_status_id = 8 THEN 1 ELSE 0 END +
                            CASE WHEN final_status_id = 1 THEN 1 ELSE 0 END) AS under_inquiry_vcm,
                        SUM(CASE WHEN final_status_id = 7 THEN 1 ELSE 0 END) AS escalated_vcm,
                        SUM(CASE WHEN final_status_id = 2 THEN 1 ELSE 0 END +
                            CASE WHEN final_status_id = 3 THEN 1 ELSE 0 END +
                            CASE WHEN final_status_id IN (5, 9, 10, 11) THEN 1 ELSE 0 END) AS fir_vcm,
                        SUM(CASE WHEN final_status_id IN (5, 9, 10, 11) THEN 1 ELSE 0 END) AS challan_vcm,
                        SUM(CASE WHEN final_status_id = 6 THEN 1 ELSE 0 END) AS resolved_vcm
                    FROM case_final_status
                    WHERE created_at BETWEEN %s AND %s
                    AND district_id IS NOT NULL
                    {district_condition}
                """
        db_cursor.execute(vcm_query, (current_date_str, to_date_str))
        values = db_cursor.fetchone()

        (received_vcm_cases, under_inquiry_vcm_cases, escalated_vcm_cases,
         fir_registered_vcm_cases, challan_submitted_vcm_cases, resolved_vcm_cases) = [
            str(val).encode('utf-8').decode('utf-8') if isinstance(val, Decimal) else val
            for val in values
        ]

        cases_query = f"""
                    SELECT pucar_case_number, level3_case_nature, pucar_caller_name, pucar_cli,
                        pucar_police_station, final_status_remarks, pucar_cro_comments, pucar_district, created_at
                    FROM case_final_status
                    WHERE created_at BETWEEN %s AND %s
                    {district_condition}
                """
        db_cursor.execute(cases_query, (current_date_str, to_date_str))
        cases = db_cursor.fetchall()

        cases_list = [
            {
                "case_number": case_number,
                "case_nature": level3_case_nature,
                "caller_name": caller_name,
                "cli": caller_number,
                "police_station": police_station,
                "district": district,
                "description": description,
                "created_at": created_at,
                "status": final_status_remarks
            }
            for (case_number, level3_case_nature, caller_name, caller_number,
                 police_station, final_status_remarks, description, district, created_at) in cases
        ]

        response = {"status": "success",
                    "data": {
                        'stats': {
                            'recieved_cases': received_vcm_cases if received_vcm_cases else 0,
                            'under_inquiry_cases': int(under_inquiry_vcm_cases if under_inquiry_vcm_cases else 0) +
                                                   int(escalated_vcm_cases if escalated_vcm_cases else 0),
                            'escalated_cases': escalated_vcm_cases if escalated_vcm_cases else 0,
                            'fir_registered': fir_registered_vcm_cases if fir_registered_vcm_cases else 0,
                            'vwps_challan_count': challan_submitted_vcm_cases if challan_submitted_vcm_cases else 0,
                            'resolved_cases': resolved_vcm_cases if resolved_vcm_cases else 0
                        },
                        'cases': cases_list,
                    },
                    "message": "VCM STATS AND CASES fetched successfully", }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)  # Properly return to the pool without removing it


@app.route(configs.CRIME_TRENDS['ENDPOINT'], methods=[configs.CRIME_TRENDS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def crime_trends():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    master_db_connection = db_config.get_db_connection()
    try:
        district_str = request.form.get('district')
        police_station_str = request.form.get('police_station')
        selected_category = request.form.get('category')
        year = request.form.get('year')
        time_period = request.form.get('time_period')  # 'week' or 'month'
        user_name = request.form.get('username')

        police_stations = police_station_str.split(",") if (police_station_str and police_station_str != 'null') else []

        if not user_name:
            return jsonify({
                'status': False,
                'message': 'Missing required parameters'
            }), 400

        if district_str or police_station_str:
            user_query = """
                    SELECT assigned_district_emergency, assigned_ps_emergency 
                    FROM 15_stats_users
                    WHERE user_name_emergency = %s
            """
            master_cursor = master_db_connection.cursor()
            master_cursor.execute(user_query, (user_name,))
            user = master_cursor.fetchone()

            if user:
                if isinstance(user[0], bytes):
                    districts = user[0].decode('utf-8')
                    assigned_districts = districts.split(',')

                if district_str not in assigned_districts:
                    return jsonify({
                        'status': False,
                        'message': "User doesn't have Access to this District"
                    }), 400
            else:
                return jsonify({
                    'status': False,
                    'message': "User not found"
                }), 400

        case_types = []
        if selected_category == 'crime_against_property':
            case_types = [
                'dacoity', 'burglary', 'robbery_snatching', 'motorcycle_theft',
                'car_theft', 'vehicle_theft', 'vehicle_snatching', 'car_snatching',
                'motorcycle_snatching'
            ]
        elif selected_category == 'crime_against_person':
            case_types = ['murder', 'firing', 'sexual_assault', 'kidnapping']
        elif selected_category:
            case_types = selected_category.split(',')
        else:  # Default: all except 'other'
            case_types = [
                'dacoity', 'burglary', 'robbery_snatching', 'motorcycle_theft',
                'car_theft', 'vehicle_theft', 'vehicle_snatching', 'car_snatching',
                'motorcycle_snatching', 'murder', 'firing', 'sexual_assault', 'kidnapping'
            ]

        # Build WHERE conditions dynamically
        conditions = [
            "district_id IS NOT NULL",
            "police_station IS NOT NULL",
            "parent_id = 0",
            "DATE(date) > '2024-06-01'"
        ]
        params = {'case_types': case_types}

        if district_str:
            district_id = configs.REVERSED_DISTRICTS_DICTIONARY.get(district_str)
            conditions.append("district_id = %(district_id)s")
            params['district_id'] = district_id

        if police_stations:
            conditions.append("police_station = ANY(%(police_stations)s)")
            params['police_stations'] = police_stations

        # if year:
        #     conditions.append("EXTRACT(YEAR FROM DATE(date)) = %(year)s")
        #     params['year'] = year

        where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""

        if time_period == 'week':
            time_grouping = "TO_CHAR(DATE_TRUNC('week', date::DATE), 'YYYY-MM-DD')"
        else:  # Default to month
            time_grouping = "TO_CHAR(date::DATE, 'YYYY-MM')"

        # Main query for crime trends
        query = f"""
                    WITH categorized_crimes AS (
                        SELECT 
                            DATE(date) AS date,
                            CASE 
                                WHEN level2_case_nature = 'Robbery/Snatching' THEN 'robbery_snatching'
                                WHEN level2_case_nature = 'Dacoity' THEN 'dacoity'
                                WHEN level3_case_nature = 'Motorcycle Theft' THEN 'motorcycle_theft'
                                WHEN level3_case_nature IN ('Cycle Theft','Other Vehicles Theft') THEN 'vehicle_theft'
                                WHEN level2_case_nature = 'Burglary' THEN 'burglary'
                                WHEN level3_case_nature = 'Car Theft' THEN 'car_theft'
                                WHEN level3_case_nature = 'Motorcycle Snatching' THEN 'motorcycle_snatching'
                                WHEN level3_case_nature = 'Car Snatching' THEN 'car_snatching'
                                WHEN level2_case_nature = 'Vehicle Snatching' THEN 'vehicle_snatching'
                                WHEN level2_case_nature = 'Murder' THEN 'murder'
                                WHEN level2_case_nature = 'Kiddnapping / Abduction' THEN 'kidnapping'
                                WHEN level2_case_nature = 'Sexual Assault' THEN 'sexual_assault'
                                WHEN level3_case_nature = 'Aerial Firing' THEN 'firing'
                                ELSE 'other'
                            END AS case_type
                        FROM response_time
                        {where_clause}
                    )
                    SELECT 
                        {time_grouping} AS time_period,
                        COUNT(*) AS total_crimes,
                        ROUND(
                            (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY {time_grouping})) * 100.0 / 
                            NULLIF(LAG(COUNT(*)) OVER (ORDER BY {time_grouping}), 0),
                            2
                        ) AS percentage_change
                    FROM categorized_crimes
                    WHERE case_type = ANY(%(case_types)s)
                    GROUP BY {time_grouping}
                    ORDER BY {time_grouping};
                """

        processed_db_cursor.execute(query, params)
        data = processed_db_cursor.fetchall()

        crime_time_periods = [row[0] for row in data]
        total_crimes = [row[1] for row in data]
        crime_pct_changes = [float(row[2]) if row[2] is not None else None for row in data]

        result_crime_trends = []
        for period, total, change in zip(crime_time_periods, total_crimes, crime_pct_changes):
            result_crime_trends.append({
                'time_period': period,
                'count': total,
                'percentage_change': change
            })

        categories = selected_category.split(',') if selected_category else []

        if selected_category == 'crime_against_property':
            columns_to_sum = (
                "SUM(dacoity) + SUM(burglary) + SUM(robbery_snatching) + "
                "SUM(motorcycle_theft) + SUM(car_theft) + SUM(vehicle_theft) + "
                "SUM(vehicle_snatching) + SUM(car_snatching) + SUM(motorcycle_snatching)"
            )

        elif selected_category == 'crime_against_person':
            columns_to_sum = (
                "SUM(murder) + SUM(firing) + SUM(sexual_assault) + SUM(kidnapping)"
            )

        elif categories:  # Handle comma-separated multiple categories
            columns_to_sum = " + ".join([f"SUM({category.strip()})" for category in categories])

        else:  # Default case: Sum all categories
            columns_to_sum = (
                "SUM(dacoity) + SUM(burglary) + SUM(robbery_snatching) + "
                "SUM(motorcycle_theft) + SUM(car_theft) + SUM(vehicle_theft) + "
                "SUM(vehicle_snatching) + SUM(car_snatching) + SUM(motorcycle_snatching) + "
                "SUM(murder) + SUM(firing) + SUM(sexual_assault) + SUM(kidnapping)"
            )

        fir_query = f"""
        SELECT 
            {time_grouping} AS time_period,
            ({columns_to_sum}) AS total_fir_cases,
            ROUND(
                (
                    ({columns_to_sum}) - 
                    LAG({columns_to_sum}) OVER (
                        ORDER BY {time_grouping} ASC
                    )
                ) * 100.0 / 
                NULLIF(
                    LAG({columns_to_sum}) OVER (
                        ORDER BY {time_grouping} ASC
                    ), 0
                ), 2
            ) AS percentage_change
        FROM fir_trends
        WHERE 1=1
        """

        if district_str:
            district_id = configs.REVERSED_DISTRICTS_DICTIONARY.get(district_str)
            fir_query += f" AND district_id = {district_id}"

        if police_station_str and police_station_str != 'null':
            fir_query += f" AND police_station = '{police_station_str}'"

        # if year:
        #     fir_query += f" AND EXTRACT(YEAR FROM date::DATE) = {year}"

        fir_query += f"""
                GROUP BY {time_grouping}
                ORDER BY {time_grouping} ASC;
                """

        processed_db_cursor.execute(fir_query)
        fir_data = processed_db_cursor.fetchall()

        fir_time_periods = [row[0] for row in fir_data]
        total_firs = [row[1] for row in fir_data]
        fir_pct_changes = [float(row[2]) if row[2] is not None else None for row in fir_data]

        fir_trends = []
        for period, total, change in zip(fir_time_periods, total_firs, fir_pct_changes):
            fir_trends.append({
                'time_period': period,
                'count': total,
                'percentage_change': change
            })

        response = {
            "data": {
                'crime_trends': result_crime_trends,
                'fir_trends': fir_trends
            },
            "status": "success",
            "message": "Crime and FIR trends fetched successfully"
        }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500

    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)
        master_cursor.close()
        master_db_connection.close()


@app.route(configs.BLOOD_DONATION['ENDPOINT'], methods=[configs.BLOOD_DONATION['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def blood_donation():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    db_conn = db_config.get_blood_db_connection()
    db_cursor = db_conn.cursor()
    try:
        district_str = request.form.get('district')
        police_station_str = request.form.get('police_station')
        view_role = request.form.get('view_role', type=int)

        current_date = datetime.now().strftime('%Y-%m-%d')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        if not all([district_str, police_station_str, view_role]):
            return jsonify({
                'status': False,
                'message': 'Missing required parameters',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        if view_role == 5 and district_ids:
            district_condition = (
                f"  AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station_name IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            district_condition = f" AND district_id IN ({', '.join(map(str, district_ids))})"
        else:
            district_condition = ""

        db_cursor.execute(f"""
            SELECT 
                COUNT(*) AS total_request_count,
                SUM(CASE WHEN final_status_id = 3 THEN 1 ELSE 0 END) AS blood_donated,
                SUM(CASE WHEN final_status_id = 2 THEN 1 ELSE 0 END) AS connected_with_donor,
                SUM(CASE WHEN final_status_id = 1 THEN 1 ELSE 0 END) AS pending_request,
                SUM(CASE WHEN final_status_id = 4 THEN 1 ELSE 0 END) AS withdrawn_by_caller,
                SUM(CASE WHEN DATE(created_at) = %s THEN 1 ELSE 0 END) AS today_request_count
            FROM requests
            WHERE 1=1
            {district_condition}
        """, (current_date,))

        result = db_cursor.fetchone()
        result = [int(value) if isinstance(value, Decimal) else value for value in result]

        # Assign to stats_array
        stats_array = {}
        stats_array['total_requests'] = result[0]
        stats_array['blood_donated'] = result[1]
        stats_array['connected_to_donor'] = result[2]
        stats_array['closed'] = result[1] + result[4]
        stats_array['pending'] = result[3]
        stats_array['withdrawn_by_caller'] = result[4]
        stats_array['today_request_recieved_count'] = result[5]

        db_cursor.execute(f"""
        SELECT 
        COUNT(*) AS active_donors_count,
        SUM(CASE WHEN DATE(last_donated_at) = %s THEN 1 ELSE 0 END) AS today_blood_donated
        FROM donars
        WHERE is_active = 1 OR DATE(last_donated_at) = %s
        """, (current_date, current_date))

        result = db_cursor.fetchone()
        result = [int(value) if isinstance(value, Decimal) else value for value in result]
        stats_array['donors_registered'] = result[0]
        stats_array['blood_donated_today'] = result[1]

        db_cursor.execute(f"""
            SELECT final_status_name, contact_person_name, contact_person_phone, 
                   required_blood_group, hospital_address, blood_required_date_time , created_at
            FROM requests
            WHERE 1=1
            {district_condition}
        """)
        requests_details = db_cursor.fetchall()

        # Storing the data in a list of dictionaries
        requests_list = [
            {
                "status": final_status_name,
                "caller_name": contact_person_name,
                "caller_phone": contact_person_phone,
                "bloodgroup_required": required_blood_group,
                "address": hospital_address,
                "donation_date": blood_required_date_time.strftime("%Y-%m-%d %H:%M:%S"),
                "case_created_at": created_at.strftime("%Y-%m-%d %H:%M:%S")
            }

            for (final_status_name, contact_person_name, contact_person_phone,
                 required_blood_group, hospital_address, blood_required_date_time, created_at) in requests_details
        ]

        db_cursor.execute(f"""
                    SELECT district_name , COUNT(*)
                    FROM requests
                    WHERE 1=1
                    {district_condition}
                    group BY  district_name
        """, )
        districtwise_rows = db_cursor.fetchall()
        districtwise_dict = {district_name: count for district_name, count in districtwise_rows}

        response = {
            "status": "success",
            "data": {
                'blood_donation_stats': stats_array,
                'cases': requests_list,
                'districtwise_donors': districtwise_dict
            },
            "message": "Blood Donation Stats and  Cases fetched successfully"
        }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it


@app.route(configs.ESCALATED_CASES['ENDPOINT'], methods=[configs.ESCALATED_CASES['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def escalated_cases():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    vccs_conn = db_config.get_vccs_db_connection()
    vccs_cursor = vccs_conn.cursor()
    vwps_conn = db_config.get_vwps_db_connection()
    vwps_cursor = vwps_conn.cursor()
    vcm_conn = db_config.get_vcm_db_connection()
    vcm_cursor = vcm_conn.cursor()
    try:
        category = request.form.get('category')
        from_date_str = request.form.get('fromDate')
        to_date_str = request.form.get('toDate')
        view_role = request.form.get('view_role', type=int)
        district_str = request.form.get('district')
        police_station_str = request.form.get('police_station')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        # Validate date format
        try:
            datetime.strptime(from_date_str, configs.YM_DATE)
            datetime.strptime(to_date_str, configs.YM_DATE)
        except ValueError:
            return jsonify({
                'status': False,
                'message': 'Invalid date format. Use YYYY-MM-DD',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        from_date_obj = datetime.strptime(from_date_str, '%Y-%m-%d')
        to_date_obj = datetime.strptime(to_date_str, '%Y-%m-%d')

        current_date_str = from_date_obj.strftime('%Y-%m-%d 00:00:00')
        to_date_str = to_date_obj.strftime('%Y-%m-%d 23:59:59')

        if view_role == 5 and district_ids:
            district_condition = (
                f"AND pucar_district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND pucar_police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )
        elif view_role in [3, 4]:
            district_condition = f"AND pucar_district_id IN ({', '.join(map(str, district_ids))})"
        else:
            district_condition = ""

        query = f"""
            SELECT pucar_case_number, level3_case_nature, pucar_caller_name, pucar_cli,
                        pucar_police_station, final_status_remarks, pucar_cro_comments, pucar_district, created_at
            FROM case_final_status
            WHERE final_status_id = 7
            AND created_at BETWEEN %s AND %s
            {district_condition}
        """

        if category == 'vccs':
            vccs_cursor.execute(query, (current_date_str, to_date_str))
            results = vccs_cursor.fetchall()
        elif category == 'vwps':
            vwps_cursor.execute(query, (current_date_str, to_date_str))
            results = vwps_cursor.fetchall()
        elif category == 'vcm':
            vcm_cursor.execute(query, (current_date_str, to_date_str))
            results = vcm_cursor.fetchall()
        else:
            results = []
            print("Invalid category provided!")

        columns = [
            "pucar_case_number", "level3_case_nature", "pucar_caller_name", "pucar_cli",
            "pucar_police_station", "final_status_remarks", "pucar_cro_comments",
            "pucar_district", "created_at"
        ]

        # Convert results into a list of dictionaries
        result_dicts = [
            {**dict(zip(columns[:-1], row[:-1])), "created_at": row[-1].strftime("%Y-%m-%d %H:%M:%S")}
            for row in results
        ]

        response = {
            "status": "success",
            "data": result_dicts,
            "message": "Escalated Cases fetched successfully"
        }
        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        vwps_cursor.close()
        vwps_conn.close()
        vccs_cursor.close()
        vccs_conn.close()
        vcm_cursor.close()
        vcm_conn.close()


@app.route(configs.CRIME_TREND_CASES['ENDPOINT'], methods=[configs.CRIME_TREND_CASES['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def crime_trend_cases():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    try:
        district_str = request.form.get('district')
        date = request.form.get('date')
        category = request.form.get('category')
        police_station = request.form.get('police_station')

        police_stations = police_station.split(",") if police_station else []

        if len(date) == 7:  # Monthly format "YYYY-MM"
            start_date, end_date = utils.get_month_range(date)
        elif len(date) == 10:  # Weekly format "YYYY-MM-DD"
            start_date, end_date = utils.get_week_range(date)
        else:
            return jsonify({"error": "Invalid date format. Use 'YYYY-MM' for monthly or 'YYYY-MM-DD' for weekly."}), 400

        category_condition = utils.get_category_condition(category)
        if not category_condition:
            return jsonify({"error": f"Invalid category: {category}"}), 400

        query_cases = f"""
            SELECT 
                case_number, level3_case_nature, caller_name, caller_number,
                accepted_time, police_station, district_id, time_id,
                description, first_arrival_time, response_time
            FROM response_time
            WHERE district_id = %s
            AND police_station is NOT NULL
            AND parent_id = 0
             {category_condition}
            AND date BETWEEN %s AND %s
        """

        if police_station and police_station != 'null':
            query_cases += f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"

        params = [configs.REVERSED_DISTRICTS_DICTIONARY[district_str], start_date, end_date]

        processed_db_cursor.execute(query_cases, params)
        cases = processed_db_cursor.fetchall()

        cases_list = [
            {
                "case_number": case_number,
                "case_nature": level3_case_nature,
                "caller_name": caller_name,
                "assigned_time": created_time,
                "cli": caller_number,
                "police_station": police_station,
                "status": 'Completed',
                "district": configs.DISTRICTS_DICTIONARY.get(int(district_id)),
                "time_id": datetime.fromtimestamp(int(time_id)).strftime(configs.YMD_HMS),
                "description": description,
                "reached_time": datetime.fromtimestamp(int(reached_time)).strftime(
                    configs.YMD_HMS) if reached_time else None,
                "response_time": f"{int(response_time // 60)}:{int(response_time % 60):02d}" if response_time else 0
            }
            for (case_number, level3_case_nature, caller_name, caller_number,
                 created_time, police_station, district_id, time_id,
                 description, reached_time, response_time) in cases
        ]

        response = {
            "success": True,
            "data": cases_list,
            "message": "Cases for Crime Trends fetched Successfuly"
        }
        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


def get_filtered_users(view_role, district, police_station):
    try:

        users_db_conn, usersdb_cursor = get_users_db_connection()

        # Base query to filter users by district or police station using FIND_IN_SET
        query = """
               SELECT user_id_emergency, first_name_emergency, last_name_emergency, user_name_emergency, view_role_emergency
                FROM users
                WHERE
                    EXISTS (
                        SELECT 1
                        FROM regexp_split_to_table(assigned_district_emergency, ',') AS district
                        WHERE district = %s
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM regexp_split_to_table(assigned_ps_emergency, ',') AS ps
                        WHERE ps = %s
                    );
               """
        params = (district, police_station)

        # Execute base query
        usersdb_cursor.execute(query, params)
        users = usersdb_cursor.fetchall()

        # Apply role-based filtering using tuple indices
        filtered_users = []
        for user in users:
            try:
                user_role = int(user[4])
                if user_role == "":
                    continue
            except (ValueError, TypeError):
                user_role = None

            # Role-based filtering
            if view_role == 2:
                filtered_users.append({
                    'user_id': user[0],
                    'first_name': user[1],
                    'last_name': user[2],
                    'user_name': user[3]
                })
            elif view_role == 3 and user_role not in (2, 3):
                filtered_users.append({
                    'user_id': user[0],
                    'first_name': user[1],
                    'last_name': user[2],
                    'user_name': user[3]
                })
            elif view_role == 4 and user_role not in (2, 3, 4):
                filtered_users.append({
                    'user_id': user[0],
                    'first_name': user[1],
                    'last_name': user[2],
                    'user_name': user[3]
                })
            elif view_role == 5 and user_role not in (2, 3, 4) and "sdpo" not in user[3]:
                filtered_users.append({
                    'user_id': user[0],
                    'first_name': user[1],
                    'last_name': user[2],
                    'user_name': user[3]
                })

        return filtered_users

    except Exception as e:
        print(f"Error: {e}")
        return []
    finally:
        usersdb_cursor.close()
        usersdb_pool.putconn(users_db_conn)


@app.route('/get_remarks_user', methods=['POST'])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def get_users():
    try:
        # Extract input parameters from form data
        user_name = request.form.get('user_name')
        view_role = int(request.form.get('view_role'))
        district = request.form.get('district')
        police_station = request.form.get('police_station')

        if not all([user_name, view_role, district, police_station]):
            return jsonify({"error": "Missing required fields."}), 400

        # Get filtered users
        filtered_users = get_filtered_users(view_role, district, police_station)

        return jsonify(filtered_users), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


def get_chat_users(view_role, district, police_station):
    try:
        users_db_conn, usersdb_cursor = get_users_db_connection()

        # Base query to filter users by district or police station using FIND_IN_SET
        query = """
               SELECT user_id_emergency, first_name_emergency, last_name_emergency, user_name_emergency, view_role_emergency
                FROM users
                WHERE
                    EXISTS (
                        SELECT 1
                        FROM regexp_split_to_table(assigned_district_emergency, ',') AS district
                        WHERE district = %s
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM regexp_split_to_table(assigned_ps_emergency, ',') AS ps
                        WHERE ps = %s
                    );
               """
        params = (district, police_station)

        # Execute base query
        usersdb_cursor.execute(query, params)
        users = usersdb_cursor.fetchall()

        # Apply role-based filtering using tuple indices
        filtered_users = []
        for user in users:
            try:
                user_role = int(user[4])
                if user_role == "":
                    continue
            except (ValueError, TypeError):
                user_role = None

            # Role-based filtering
            if view_role == 2:
                filtered_users.append({
                    'user_id': user[0],
                    'first_name': user[1],
                    'last_name': user[2],
                    'user_name': user[3]
                })
            elif view_role == 3 and user_role not in (2, 3):
                filtered_users.append({
                    'user_id': user[0],
                    'first_name': user[1],
                    'last_name': user[2],
                    'user_name': user[3]
                })
            elif view_role == 4 and user_role not in (2, 3, 4):
                filtered_users.append({
                    'user_id': user[0],
                    'first_name': user[1],
                    'last_name': user[2],
                    'user_name': user[3]
                })
            elif view_role == 5 and user_role not in (2, 3, 4) and "sdpo" not in user[3]:
                filtered_users.append({
                    'user_id': user[0],
                    'first_name': user[1],
                    'last_name': user[2],
                    'user_name': user[3]
                })

        return filtered_users

    except Exception as e:
        print(f"Error: {e}")
        return []
    finally:
        usersdb_cursor.close()
        usersdb_pool.putconn(users_db_conn)


@app.route('/get_chat_user', methods=['POST'])
# @limiter.limit(configs.LIMITER)
# @require_api_key
# @jwt_required()
def get_chat_user():
    try:
        # Extract input parameters from form data
        user_name = request.form.get('user_name')
        view_role = int(request.form.get('view_role'))
        district = request.form.get('district')
        police_station = request.form.get('police_station')

        if not all([user_name, view_role, district, police_station]):
            return jsonify({"error": "Missing required fields."}), 400

        # Get filtered users
        filtered_users = get_chat_users(view_role, district, police_station)

        return jsonify(filtered_users), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route(configs.CRIME_REOCCURENCE_CASE['ENDPOINT'],
           methods=[configs.CRIME_REOCCURENCE_CASE['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def crime_reoccurrence_case():
    """Get detailed information for a specific case number.

    Expected POST body:
    {
        "case_number": "string",      # The case number to retrieve details for
        "assigned_to": "string",      # Optional; if empty, use the current username or handle differently
        "assigned_by": "string"       # Optional; if empty, use the current username or handle differently
    }

    Returns:
        JSON: Case details with a standardized response format.
              When neither assigned parameter is provided, an additional key
              'assigned_by_users' lists all users to whom the current user has assigned the case.
    """
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    db_conn = db_config.get_db_connection()
    db_cursor = db_conn.cursor()

    try:
        username = get_jwt_identity()

        case_number_str = str(request.form.get('case_number'))
        assigned_to_param = request.form.get('assigned_to')
        assigned_by_param = request.form.get('assigned_by')

        case_query = """
            SELECT 
                case_number, description, response_time, 
                responder_id, accepted_time, police_station,
                district_id, region_category, caller_name,
                caller_number, caller_location, level3_case_nature, 
                dispatched_time, first_arrival_time,lat, long, 
                responder_lat, responder_long
            FROM response_time
            WHERE case_number = %s
        """
        processed_db_cursor.execute(case_query, (case_number_str,))
        case_details = processed_db_cursor.fetchone()

        if not case_details:
            return jsonify({
                'status': False,
                'message': 'Case not found',
                'data': None
            }), 404

        # Unpack the case details
        (case_number, description, response_time, responder_id, accepted_time, police_station,
         district_id, region_category, caller_name, caller_number, caller_location,
         level3_case_nature, dispatched_time, first_arrival_time,
         lat, long, responder_lat, responder_long) = case_details

        # Query remarks (if applicable) or get assigned users list ---
        # Initialize variables that will be used in the response.
        new_remarks = ""
        new_assignedby = None
        new_assignedto = None
        assigned_users = None  # extra key added only in one scenario

        # Neither assigned_to nor assigned_by provided.
        if not assigned_to_param and not assigned_by_param:
            assigned_query = """
                SELECT DISTINCT assigned_to
                FROM remarks
                WHERE case_id = %s AND assigned_by = %s
            """
            processed_db_cursor.execute(assigned_query, (case_number_str, username))
            assigned_records = processed_db_cursor.fetchall()
            assigned_users = [rec[0] for rec in assigned_records] if assigned_records else []
            # For this example, we leave remarks empty and record that the current user is the assigner.
            new_assignedby = username
            new_assignedto = None
        else:
            # one of assigned_to or assigned_by is provided.
            if not assigned_to_param:
                assigned_to = username
                assigned_by = assigned_by_param
            elif not assigned_by_param:
                assigned_by = username
                assigned_to = assigned_to_param
            else:
                assigned_to = assigned_to_param
                assigned_by = assigned_by_param

            remarks_query = """
                SELECT remarks, assigned_by, assigned_to
                FROM remarks
                WHERE case_id = %s AND assigned_to = %s AND assigned_by = %s
                LIMIT 1
            """
            processed_db_cursor.execute(remarks_query, (case_number_str, assigned_to, assigned_by))
            remarks_record = processed_db_cursor.fetchone()

            if remarks_record:
                new_remarks = remarks_record[0]
                new_assignedby = remarks_record[1]
                new_assignedto = remarks_record[2]
            else:
                new_remarks = ""
                new_assignedby = assigned_by
                new_assignedto = assigned_to

        db_cursor.execute(f"""
                            SELECT matched_coordinates, related_cases 
                            FROM pred_pol_crimes_hotspot 
                            WHERE case_number = %s
                """, (case_number,))
        re_occurrences_cases = db_cursor.fetchone()

        if re_occurrences_cases:
            matched_coordinates, related_cases = re_occurrences_cases
            matched_coordinates_str = matched_coordinates.decode('utf-8') if isinstance(matched_coordinates,
                                                                                        bytes) else matched_coordinates
            related_cases_str = related_cases.decode('utf-8') if isinstance(related_cases, bytes) else related_cases

            matched_coordinates = json.loads(matched_coordinates_str)
            related_cases = ast.literal_eval(related_cases_str)
            updated_coordinates = []
            for related_case, coordinate in zip(related_cases, matched_coordinates):
                processed_db_cursor.execute(
                    "SELECT accepted_time FROM response_time WHERE case_number = %s",
                    (related_case,)
                )
                accepted_time_record = processed_db_cursor.fetchone()
                accepted_time = accepted_time_record[0] if accepted_time_record else None
                # Append the accepted_time as the third element to the coordinate list.
                updated_coordinates.append(coordinate + [accepted_time])
            matched_coordinates = updated_coordinates
        else:
            # Handle the case where no data is returned
            matched_coordinates, related_cases = None, None

        case_response = {
            'case_number': case_number,
            'description': description,
            'response_time': f"{int(response_time // 60)}:{int(response_time % 60):02d}" if response_time else "0:00",
            'responder_id': responder_id,
            'accepted_time': accepted_time,
            'police_station': police_station,
            'district_id': configs.DISTRICTS_DICTIONARY.get(district_id),
            'region_category': region_category,
            'caller_name': caller_name,
            'caller_number': caller_number,
            'caller_location': caller_location,
            'level3_case_nature': level3_case_nature,
            'remarks': utils.parse_remarks(new_remarks),
            'assigned_by': new_assignedby,
            'assigned_to': new_assignedto,
            'dispatched_time': (datetime.fromtimestamp(int(dispatched_time)).strftime("%d %b %Y %H:%M:%S")
                                if dispatched_time is not None else 'N/A'),
            'first_arrival_time': (datetime.fromtimestamp(int(first_arrival_time)).strftime("%d %b %Y %H:%M:%S")
                                   if first_arrival_time is not None else 'N/A'),
            'lat': lat,
            'long': long,
            'responder_lat': responder_lat,
            'responder_long': responder_long,
            're-occurence_coordinates': matched_coordinates,
            're-occurrence_case_numbers': related_cases
        }

        # incase assigned_by and assigned_to are not provided
        if assigned_users is not None:
            case_response['assigned_by_users'] = assigned_users

        response = {
            'status': True,
            'message': 'Case details fetched successfully',
            'data': case_response
        }
        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': f'Internal server error: {e}',
            'data': None
        }), 500

    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.PS_CONFERENCE_CALL_STATS['ENDPOINT'], methods=[configs.PS_CONFERENCE_CALL_STATS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def ps_conference_call_stats():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()

    try:
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        district_str = request.form.get('district')

        if district_str:
            district_id = configs.REVERSED_DISTRICTS_DICTIONARY.get(district_str)

            dist_conf_query = """
                    SELECT 
                        police_station,
                        SUM(CASE WHEN field3 = 'Successful Conference call' THEN 1 ELSE 0 END) AS successful_calls,
                        SUM(CASE WHEN field3 IN ('FO did not attend the call','Number Powered Off') THEN 1 ELSE 0 END) AS Unsuccessful_calls
                    FROM 
                        response_time
                    WHERE 
                        date BETWEEN %s AND %s
                        AND field3 IS NOT NULL
                        AND district_id = %s
                        AND parent_id = 0
                    GROUP BY 
                        police_station;
                     """

            processed_db_cursor.execute(dist_conf_query, (from_date, to_date, district_id))
            dist_conf_results = processed_db_cursor.fetchall()

            district_response = {
                ps[0]: {
                    'successful': ps[1],
                    'unsuccessful': ps[2]
                }
                for ps in dist_conf_results
            }

            response = {
                'status': True,
                'message': 'PS Conference calls Stats fetched successfully',
                'data': district_response
            }
            return jsonify(response), 200

        else:
            return jsonify({
                "success": False,
                "message": "Provide a Valid District."
            }), 500
    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.CALLER_FEEDBACK['ENDPOINT'], methods=[configs.CALLER_FEEDBACK['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def caller_feedback_districtwise():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    try:
        district_str = request.form.get('district')
        view_role = request.form.get('view_role', type=int)

        districts = district_str.split(",") if district_str else []

        currnt_date = datetime.now().strftime(configs.YM_DATE)

        district_condition = ""
        if view_role in [3, 4]:
            district_condition = f""" AND district IN ({', '.join(f"'{district}'" for district in districts)})"""

        processed_db_cursor.execute(f"""
                            SELECT district_id,
                                SUM(CASE WHEN caller_feedback = 'Positive' THEN 1 ELSE 0 END) as positive,
                                SUM(CASE WHEN caller_feedback = 'Negative' THEN 1 ELSE 0 END) as negative,
                                SUM(CASE WHEN caller_feedback = 'Not Responding' THEN 1 ELSE 0 END) as not_responding
                            FROM
                                response_time
                            WHERE 
                                date = %s
                            {district_condition}
                            Group By district_id
                        """, (currnt_date,))

        district_feedback_stats = processed_db_cursor.fetchall()

        existing_stats = {
            configs.DISTRICTS_DICTIONARY.get(row[0]): {"district": configs.DISTRICTS_DICTIONARY.get(row[0]),
                                                       "positive": row[1],
                                                       "negative": row[2], "not_responding": row[3]}
            for row in district_feedback_stats}

        dist_stats = []
        for district in districts:
            dist_stats.append(
                existing_stats.get(
                    district,
                    {
                        "district": district,
                        "positive": 0,
                        "negative": 0,
                        "not_responding": 0,
                    },
                )
            )

        processed_db_conn.close()

        total_positive = sum(row["positive"] for row in dist_stats)
        total_negative = sum(row["negative"] for row in dist_stats)
        total_not_responding = sum(row["not_responding"] for row in dist_stats)

        response = {"status": "success",
                    "data": {
                        'district_feedack': dist_stats,
                        'total': {
                            'total_positive': total_positive,
                            'total_negative': total_negative,
                            'total_not_responding': total_not_responding
                        }
                    },
                    "message": "Caller Feedback fetched successfully"}

        return jsonify(response), 200


    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.CALLER_FEEBACK_PSWISE['ENDPOINT'], methods=[configs.CALLER_FEEBACK_PSWISE['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def caller_feedback_pswise():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    try:
        district_str = request.form.get('district')

        currnt_date = datetime.now().strftime(configs.YM_DATE)

        district_id = configs.REVERSED_DISTRICTS_DICTIONARY.get(district_str)

        processed_db_cursor.execute(f"""
                            SELECT police_station,
                                SUM(CASE WHEN caller_feedback = 'Positive' THEN 1 ELSE 0 END) as positive,
                                SUM(CASE WHEN caller_feedback = 'Negative' THEN 1 ELSE 0 END) as negative,
                                SUM(CASE WHEN caller_feedback = 'Not Responding' THEN 1 ELSE 0 END) as not_responding
                            FROM
                                response_time
                            WHERE 
                                date = %s
                                AND district_id = %s
                            Group By police_station
                        """, (currnt_date, district_id))

        district_feedback_stats = processed_db_cursor.fetchall()

        existing_stats = [{"police_station": row[0], "positive": row[1],
                           "negative": row[2], "not_responding": row[3]}
                          for row in district_feedback_stats]

        processed_db_conn.close()

        response = {"status": "success",
                    "data": {
                        'ps_feedback': existing_stats,
                    },
                    "message": "Police station wise Caller Feedback fetched successfully"}

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            "success": False,
            "message": "An unexpected error occurred. Please try again later."
        }), 500
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.NEGATIVE_FEEDBACK_CASES['ENDPOINT'], methods=[configs.NEGATIVE_FEEDBACK_CASES['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def negative_feedback_cases():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    try:
        district_str = request.form.get('district')
        from_date = request.form.get('fromDate')
        to_date = request.form.get('toDate')
        view_role = request.form.get('view_role', type=int)
        police_station_str = request.form.get('police_station')

        districts = district_str.split(",") if district_str else []
        police_stations = police_station_str.split(",") if police_station_str else []

        if view_role not in [1, 2, 3, 4, 5]:
            return jsonify({
                'status': False,
                'message': 'Invalid view role. Use 2 or 3.',
                'data': None
            }), 400

        district_ids = []
        if districts:
            for district in districts:
                if district in configs.REVERSED_DISTRICTS_DICTIONARY:
                    district_ids.append(configs.REVERSED_DISTRICTS_DICTIONARY[district])
                else:
                    return jsonify({
                        'status': False,
                        'message': f"Invalid district name: {district}",
                        'data': None
                    }), 400

        district_condition = ""
        if (view_role == 3 or view_role == 4) and district_ids:
            district_condition = f"AND district_id IN ({', '.join(map(str, district_ids))})"
        elif view_role == 5 and district_ids:
            district_condition = (
                f"AND district_id IN ({', '.join(map(str, district_ids))}) "
                f"AND police_station IN ({', '.join([repr(ps) for ps in police_stations])})"
            )

        from_date_epoch = utils.date_to_unix_day_start_end(from_date, True)
        to_date_epoch = utils.date_to_unix_day_start_end(to_date, False)

        query = f"""
                SELECT  case_number, level3_case_nature, caller_name, caller_number,
                accepted_time, police_station, district_id, time_id,
                description, first_arrival_time, caller_location , caller_feedback
                from response_time
                where time_id::bigint BETWEEN %s AND %s 
                    AND parent_id = 0 
                    AND caller_feedback = 'Negative'
                    {district_condition}
        """
        processed_db_cursor.execute(query, (from_date_epoch, to_date_epoch))
        cases = processed_db_cursor.fetchall()

        cases_list = [
            {
                "case_number": case_number.decode() if isinstance(case_number, bytes) else case_number,
                "case_nature": level3_case_nature.decode() if isinstance(level3_case_nature,
                                                                         bytes) else level3_case_nature,
                "caller_name": caller_name.decode() if isinstance(caller_name, bytes) else caller_name,
                "assigned_time": created_time.decode() if isinstance(created_time, bytes) else created_time,
                "cli": caller_number.decode() if isinstance(caller_number, bytes) else caller_number,
                "police_station": police_station.decode() if isinstance(police_station, bytes) else police_station,
                "status": 'closed',
                "district": configs.DISTRICTS_DICTIONARY.get(int(district_id)),
                "time_id": datetime.fromtimestamp(int(time_id)).strftime(configs.YMD_HMS),
                "description": description.decode() if isinstance(description, bytes) else description,
                "reached_time": datetime.fromtimestamp(int(reached_time)).strftime(
                    configs.YMD_HMS) if reached_time else None,
                "address": address.decode() if isinstance(address, bytes) else address,
                "caller_feedback": caller_feedback.decode() if isinstance(caller_feedback, bytes) else caller_feedback
            }
            for (case_number, level3_case_nature, caller_name, caller_number,
                 created_time, police_station, district_id, time_id,
                 description, reached_time, address, caller_feedback) in cases
        ]

        response = {
            "success": True,
            "data": cases_list,
            "message": "Cases for Negative Feedbacks fetched Successfully"
        }
        return jsonify(response), 200


    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': f'Internal server error {e}'
        }), 400
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it
        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


@app.route(configs.USER_ANALYTICS['ENDPOINT'], methods=[configs.USER_ANALYTICS['METHOD']])
@limiter.limit(configs.LIMITER)
@require_api_key
@validate_ownership
def user_analytics():
    log_db_conn, log_db_cursor = get_log_pg_db_connection()
    processed_db_conn, processed_db_cursor = get_processed_db_connection()
    users_db_conn, usersdb_cursor = get_users_db_connection()
    try:
        district_str = request.form.get('district')
        view_role = request.form.get('view_role', type=int)
        username = request.form.get('username')

        districts = district_str.split(",") if district_str else []

        if view_role not in [1, 2, 3, 4]:
            return jsonify({
                'status': False,
                'message': 'Invalid User Role For User Analytics',
                'data': None
            }), 400

        district_condition = ""
        if (view_role == 3 or view_role == 4) and districts:
            district_condition = f"""AND district IN ({', '.join(f"'{district}'" for district in districts)})"""

        # Total User Status
        user_status_query = f"""
                SELECT
                    SUM(CASE WHEN district IS NOT NULL THEN 1 ELSE 0 END) AS total_users,
                    SUM(CASE WHEN DATE(lastseen) = CURRENT_DATE AND district IS NOT NULL THEN 1 ELSE 0 END) AS online,
                    SUM(CASE WHEN DATE(lastseen) <> CURRENT_DATE AND district IS NOT NULL THEN 1 ELSE 0 END) AS offline
                FROM users
                Where 1=1
                 {district_condition};

                """
        usersdb_cursor.execute(user_status_query, )
        row = usersdb_cursor.fetchone()

        if row:
            total_users = int(row[0]) if isinstance(row[0], Decimal) else row[0]
            online_users = int(row[1]) if isinstance(row[1], Decimal) else row[1]
            offline_users = int(row[2]) if isinstance(row[2], Decimal) else row[2]

        # Districtwise User Status
        district_query = f"""
                SELECT 
                  district,
                  SUM(CASE WHEN district IS NOT NULL THEN 1 ELSE 0 END) AS total_users,
                  SUM(CASE WHEN DATE(lastseen) = CURRENT_DATE AND district IS NOT NULL THEN 1 ELSE 0 END) AS online,
                  SUM(CASE WHEN DATE(lastseen) <> CURRENT_DATE AND district IS NOT NULL THEN 1 ELSE 0 END) AS offline
                FROM users
                WHERE district IS NOT NULL
                {district_condition}
                GROUP BY district;
        """
        usersdb_cursor.execute(district_query)
        district_data = usersdb_cursor.fetchall()

        district_results = []
        for row in district_data:
            district_results.append({
                'district': row[0],
                'total_users': row[1],
                'online': int(row[2]) if isinstance(row[2], Decimal) else row[2],
                'offline': int(row[3]) if isinstance(row[3], Decimal) else row[3]
            })

        query_users = """SELECT user_name_emergency
                        FROM users
                        WHERE 
                            CAST(view_role_emergency AS INTEGER) > %s
                            AND district IS NOT NULL
                            AND EXISTS (
                                SELECT 1
                                FROM UNNEST(STRING_TO_ARRAY(district, ',')) AS user_district
                                WHERE user_district = ANY (
                                    SELECT UNNEST(STRING_TO_ARRAY(assigned_district_emergency, ','))
                                    FROM users
                                    WHERE user_name_emergency = %s
                                                        )
    );"""
        usersdb_cursor.execute(query_users, (view_role,username))

        # all usernames
        usernames = [row[0] for row in usersdb_cursor.fetchall()]
        placeholders = ', '.join(['%s'] * len(usernames))

        query_remarks = (
            f"SELECT assigned_by , assigned_to , case_id FROM remarks "
            f"WHERE assigned_by IN ({placeholders}) AND time_stamp::date = CURRENT_DATE"
        )
        processed_db_cursor.execute(query_remarks, tuple(usernames))
        remarks_data = processed_db_cursor.fetchall()

        columns = [desc[0] for desc in processed_db_cursor.description]
        remarks = [dict(zip(columns, row)) for row in remarks_data]

        offline_users_query = f"""
                SELECT first_name_emergency, last_name_emergency, lastseen, district
                FROM users
                WHERE DATE(lastseen) <> CURRENT_DATE
                AND district is NOT NULL
                {district_condition};
        """
        usersdb_cursor.execute(offline_users_query)
        offline = usersdb_cursor.fetchall()

        offline_users_activity = [
            {
                "name": row[0] + ' ' + row[1],
                "lastseen": row[2].strftime("%Y-%m-%d %H:%M:%S") if isinstance(row[2], datetime) else row[2],
                "district": row[3]
            }
            for row in offline
        ]

        online_users_query = f"""
                SELECT first_name_emergency, last_name_emergency, lastseen , district
                FROM users
                WHERE DATE(lastseen) = CURRENT_DATE
                AND district is NOT NULL
                {district_condition};
        """
        usersdb_cursor.execute(online_users_query)
        online = usersdb_cursor.fetchall()

        online_users_activity = [
            {
                "name": row[0] + ' ' + row[1],
                "lastseen": row[2].strftime("%Y-%m-%d %H:%M:%S") if isinstance(row[2], datetime) else row[2],
                "district": row[3]
            }
            for row in online
        ]

        response = {
            "data": {
                "overall_stats": {
                    'total': total_users,
                    'online': online_users,
                    'offline': offline_users
                },
                "district_wise_users": district_results,
                "remarks": remarks,
                "online_users_activity": online_users_activity,
                "offline_users_activity": offline_users_activity
            }
        }

        return jsonify(response), 200

    except Exception as e:
        utils.log_to_pg_database(log_db_conn, log_db_cursor, "ERROR", traceback.format_exc(), request.remote_addr)
        return jsonify({
            'status': False,
            'message': f'Internal server error {e}'
        }), 400
    finally:
        log_db_cursor.close()
        log_db_pool.putconn(log_db_conn)
        # Properly return to the pool without removing it

        usersdb_cursor.close()
        usersdb_pool.putconn(users_db_conn)

        processed_db_cursor.close()
        postgresql_pool.putconn(processed_db_conn)


if __name__ == '__main__':
    app.run(host=configs.HOST, port=configs.PORT, debug=False)  # configs.DEBUG_
