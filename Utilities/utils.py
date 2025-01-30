from flask import jsonify, request, abort
import time
import requests
from bs4 import BeautifulSoup
from requests.exceptions import RequestException
import pickle
import os
import mysql.connector
from datetime import datetime, timedelta
from Utilities import configs
import sqlite3
from dotenv import load_dotenv
import psycopg2
import traceback
import json
import firebase_admin
from firebase_admin import messaging

load_dotenv()

SYS_IP = os.getenv("SYS_IP_ADDRESS")


def initialize_session():
    login_page_url = "https://admin15.psca.gop.pk/public/login"
    session = requests.Session()

    response = session.get(login_page_url)
    soup = BeautifulSoup(response.text, 'html.parser')
    csrf_token = soup.find('input', {'name': '_token'})['value']
    configs.AP_CREDENTIALS['_token'] = csrf_token
    response = session.post(login_page_url, data=configs.AP_CREDENTIALS)
    if response.ok:
        print("Login successful!")
        save_session(session, csrf_token)
        return session, csrf_token
    else:
        print("Login failed:", response.status_code)
        return None, None


def save_session(session, csrf_token):
    with open(configs.SESSION_FILE, 'wb') as f:
        pickle.dump((session.cookies, csrf_token), f)


def load_session():
    try:
        with open(configs.SESSION_FILE, 'rb') as f:
            cookies, csrf_token = pickle.load(f)
            session = requests.Session()
            session.cookies.update(cookies)
            return session, csrf_token
    except FileNotFoundError:
        return None, None


def fetch_data(session, csrf_token, form_data):
    view_url = "https://admin15.psca.gop.pk/public/dashboard/realtime/view"
    overall_api_url = "https://admin15.psca.gop.pk/public/dashboard/realtime/overall"

    try:
        form_data['_token'] = csrf_token
        view_response = session.post(view_url, data=form_data)
        if view_response.ok:
            overall_response = session.get(overall_api_url)
            if overall_response.ok:
                data = overall_response.json()
                return data
            else:
                print("Failed to fetch API data:", overall_response.status_code)
                return None
        else:
            print("Failed to submit form:", view_response.status_code)
            return None
    except RequestException as e:
        print(f"Request failed: {e}")
        return None


def get_data():
    session, csrf_token = load_session()

    if session is None or csrf_token is None:
        print("Session expired or not found. Reinitializing session...")
        session, csrf_token = initialize_session()

    if session is None or csrf_token is None:
        return None

    form_data = {
        'campaigns[]': configs.SKILLS
    }

    data = fetch_data(session, csrf_token, form_data)

    if data is None:
        print("Re-initializing session and fetching data...")
        session, csrf_token = initialize_session()
        if session and csrf_token:
            data = fetch_data(session, csrf_token, form_data)

    return data


def create_vehicle_session():
    """Create a new session and login."""
    session = requests.Session()
    response = session.get(configs.VEHICLE_LOGIN_URL)
    soup = BeautifulSoup(response.text, 'html.parser')
    csrf_token = soup.find('input', {'name': '_token'})['value']
    configs.V_CREDENTIALS['_token'] = csrf_token
    session.post(configs.VEHICLE_LOGIN_URL, data=configs.V_CREDENTIALS, headers=configs.HEADERS)

    return session


def save_vehicle_session(session):
    with open(configs.SESSION_VEHICLE_FILE, 'wb') as f:
        pickle.dump({
            'session': session,
            'timestamp': datetime.now(),
            'last_validation': datetime.now()
        }, f)


def load_vehicle_session():
    if os.path.exists(configs.SESSION_VEHICLE_FILE):
        with open(configs.SESSION_VEHICLE_FILE, 'rb') as f:
            data = pickle.load(f)
            if datetime.now() - data['timestamp'] < configs.SESSION_EXPIRY_TIME:
                # Check if the session is still valid by making a test request
                if datetime.now() - data['last_validation'] < configs.SESSION_CHECK_INTERVAL:
                    return data['session']  # Return valid session if checked recently
                else:
                    # Validate the session
                    if vehicle_session_validity(data['session']):
                        data['last_validation'] = datetime.now()
                        save_vehicle_session(data['session'])
                        return data['session']
    session = create_vehicle_session()
    save_vehicle_session(session)
    return session


def vehicle_session_validity(session):
    """if the session is still valid by making a small request."""
    try:
        response = session.get(configs.VEHICLE_STATS_URL, timeout=5)
        soup = BeautifulSoup(response.text, 'html.parser')
        div_class_check = soup.find('div', class_="small-box bg-purple")
        return div_class_check is not None
    except requests.RequestException:
        return False


def fetch_vehicle_stats(session):
    try:
        response = session.get(configs.VEHICLE_STATS_URL)

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            data_dict = {}
            div_classes = ["small-box bg-purple", "small-box bg-green", "small-box bg-yellow", "small-box bg-red"]
            for div_class in div_classes:
                divs = soup.find_all('div', class_=div_class)
                for div in divs:
                    h3_tags = div.find_all('h3')
                    if len(h3_tags) >= 2:
                        key = h3_tags[0].text.strip()
                        try:
                            value = int(h3_tags[1].text.strip())
                            data_dict[key] = {'count': value}
                            spans = div.find_all('span')
                            for span in spans:
                                span_text = span.text.strip()
                                span_key, span_value = span_text.split(':')
                                data_dict[key][span_key.strip()] = int(span_value.strip())
                        except Exception as e:
                            return {"error": "Failed to parse data"}
            return data_dict
        else:
            raise Exception(f"Failed to retrieve data. Status code: {response.status_code}")
    except requests.RequestException as e:
        raise Exception(f"Request failed: {str(e)}")


def date_to_unix_day_start_end(date_str, is_start=True):
    date_format = configs.YMD_HMS
    time_str = '00:00:00' if is_start else '23:59:59'
    dt = datetime.strptime(f'{date_str} {time_str}', date_format)
    return int(dt.timestamp())


def date_to_unix_time(date_str):
    return int(time.mktime(time.strptime(date_str, configs.YMD_HMS)))


def matching_category(response):
    CAW_fir_generated = 0
    CACH_fir_generated = 0
    CAP_fir_generated = 0
    CAPRO_fir_generated = 0

    CAW_cases_generated = 0
    CACH_cases_generated = 0
    CAP_cases_generated = 0
    CAPRO_cases_generated = 0

    for item in response:
        case_nature = item.get('case_nature')
        firs_generated = item.get('firs_generated', 0)
        case_generated = item.get('generated_cases', 0)

        date = item.get('date', 0)
        if case_nature in configs.CAW:
            CAW_fir_generated += firs_generated
            CAW_cases_generated += case_generated
        elif case_nature in configs.CACH:
            CACH_fir_generated += firs_generated
            CACH_cases_generated += case_generated
        elif case_nature in configs.CAP:
            CAP_fir_generated += firs_generated
            CAP_cases_generated += case_generated
        else:
            CAPRO_fir_generated += firs_generated
            CAPRO_cases_generated += case_generated
    return {
        'date': date,
        'CAW_fir_conversion': ((CAW_fir_generated / CAW_cases_generated) * 100) if CAW_cases_generated > 0 else 0,
        'CAW_fir': CAW_fir_generated,
        'CAW_cases': CAW_cases_generated,
        'CACH_fir_conversion': ((CACH_fir_generated / CACH_cases_generated) * 100) if CACH_cases_generated > 0 else 0,
        'CACH_fir': CACH_fir_generated,
        'CACH_cases': CACH_cases_generated,
        'CAP_fir_conversion': ((CAP_fir_generated / CAP_cases_generated) * 100) if CAP_cases_generated > 0 else 0,
        'CAP_fir': CAP_fir_generated,
        'CAP_cases': CAP_cases_generated,
        'CAPRO_fir_conversion': (
                (CAPRO_fir_generated / CAPRO_cases_generated) * 100) if CAPRO_cases_generated > 0 else 0,
        'CAPRO_fir': CAPRO_fir_generated,
        'CAPRO_cases': CAPRO_cases_generated
    }


def get_db_connection(database=configs.PROCESSED_STATS_MAIN):
    conn = sqlite3.connect(database)
    conn.row_factory = sqlite3.Row
    return conn


"""Custom error handler for 400 status code"""


def handle_400_error(e):
    response = e.get_response()
    response.data = jsonify({
        "message": e.description
    }).data
    response.content_type = "application/json"
    return response


# def limit_remote_addr(log_db_conn, log_db_cursor):
#     client_ip = request.remote_addr
#     if client_ip not in configs.WHITE_LISTED_IPS:
#         log_to_database(log_db_conn, log_db_cursor, "CRITICAL",
#                                       f"Unauthorized access attempt from IP: {client_ip}")
#         abort(configs.UNAUTHORIZED_REQUEST_ERROR, f"Unauthorized access attempt from IP: {client_ip}")


def case_stats_response(row, regional_avg_responses, fir_result):
    total_calls, siraiki, punjabi, potohari, english, traffic, vwps, app_alerts, transfered, call_backs, video_calls, estimated_response_time, succ_conf_calls, unsucc_conf_calls, vccs, vcm, generated_cases = row
    emergency_15 = (total_calls - vwps - traffic - vccs - vcm - generated_cases) if total_calls else None
    stats = {
        'total_calls': total_calls,
        'siraiki-15': siraiki,
        'punjabi-15': punjabi,
        'potohari-15': potohari,
        'english-15': english,
        'transferred_cases': transfered,
        'call_backs': call_backs,
        'app_alerts': app_alerts,
        'female-15_count': vwps,
        'female-15': (vwps / total_calls) * 100 if total_calls else 0,
        'traffic-15_count': traffic,
        'traffic-15': (traffic / total_calls) * 100 if total_calls else 0,
        'pucar-15_count': emergency_15,
        'pucar-15': (emergency_15 / total_calls) * 100 if total_calls else 0,
        'total_generated_cases': generated_cases,
        'fir_rate': configs.FIR_RATE * 100,
        'total_fir': int(configs.FIR_RATE * generated_cases) if generated_cases else 0,
        'video_calls': video_calls,
        'rural_response_time': f"{int(regional_avg_responses[0][1] // 60)}:{int(regional_avg_responses[0][1] % 60):02d}" if regional_avg_responses else 0,
        'urban_response_time': f"{int(regional_avg_responses[1][1] // 60)}:{int(regional_avg_responses[1][1] % 60):02d}" if len(
            regional_avg_responses) > 1 else 0,
        'estimated_response_time': estimated_response_time,
        'successful_conference': succ_conf_calls,
        'unsuccessful_conference': unsucc_conf_calls,
        'vccs-15_count': vccs,
        'vcm-15_count': vcm
        # 'CAW': fir_result[0] if fir_result else 0,
        # 'CACH': fir_result[1] if fir_result else 0,
        # 'CAP': fir_result[2] if fir_result else 0,
        # 'CAPRO':fir_result[3] if fir_result else 0
    }
    return stats


def pitb_dashboard_response(row, regional_avg_responses, total_henious_calls, from_date_str, to_date_str):
    total_calls, siraiki, punjabi, potohari, english, traffic, vwps, app_alerts, transfered, call_backs, video_calls, estimated_response_time, conference_calls = row

    emergency_15 = total_calls - vwps - traffic if total_calls is not None and vwps is not None and traffic is not None else None
    stats = {
        'total_calls': total_calls if total_calls else 0,
        'siraiki-15': siraiki if siraiki else 0,
        'punjabi-15': punjabi if punjabi else 0,
        'potohari-15': potohari if potohari else 0,
        'english-15': english if english else 0,
        'call_backs': call_backs if call_backs else 0,
        'app_alerts': app_alerts if app_alerts else 0,
        'female-15': vwps if vwps else 0,
        'traffic-15': traffic if traffic else 0,
        'pucar-15': emergency_15 if emergency_15 else 0,
        'video_calls': video_calls if video_calls else 0,
        'rural_response_time': f"{int(regional_avg_responses[0][1] // 60)}:{int(regional_avg_responses[0][1] % 60):02d}" if
        regional_avg_responses[0][1] else 0,
        'urban_response_time': f"{int(regional_avg_responses[1][1] // 60)}:{int(regional_avg_responses[1][1] % 60):02d}" if
        regional_avg_responses[1][1] else 0,
        'conference_calls': conference_calls if conference_calls else 0,
        'dacoity': total_henious_calls[0] if total_henious_calls[0] else 0,
        'robbery': total_henious_calls[1] if total_henious_calls[1] else 0,
        'murder': total_henious_calls[2] if total_henious_calls[2] else 0,
        'women_harrasment': total_henious_calls[3] if total_henious_calls[3] else 0,
        'traffic_accidents': total_henious_calls[4] if total_henious_calls[4] else 0,
        'rape': total_henious_calls[5] if total_henious_calls[5] else 0
    }
    response = {
        'message': "Data Fetched Successfully",
        'status': True,
        'data': stats
    }
    return response


def format_report_response(district, processed_data, regional_avg_response, response_time,
                           response_time_stats):
    response_data = []
    for i in range(len(processed_data)):
        processed_data_row = processed_data[i]
        if len(processed_data_row) == 16:
            list_header = district
            total_calls, siraiki, punjabi, potohari, english, traffic, vwps, app_alerts, transfered, call_backs, video_calls, estimated_response_time, conference_calls, vccs, vcm, generated_cases = processed_data_row
        else:
            list_header, total_calls, siraiki, punjabi, potohari, english, traffic, vwps, app_alerts, transfered, call_backs, video_calls, estimated_response_time, conference_calls, vccs, vcm, generated_cases = processed_data_row
        # total_generated_cases = sum(generated_cases)

        emergency_15 = None if total_calls is None else total_calls - vwps - traffic - vccs - vcm

        stats = {
            'list_header': configs.DISTRICTS_DICTIONARY.get(list_header, list_header),
            'total_calls': total_calls,
            'siraiki-15': siraiki,
            'punjabi-15': punjabi,
            'potohari-15': potohari,
            'english-15': english,
            'transferred_cases': transfered,
            'call_backs': call_backs,
            'app_alerts': app_alerts,
            'female-15_count': vwps,
            'female-15': (vwps / total_calls) * 100 if total_calls else 0,
            'traffic-15_count': traffic,
            'traffic-15': (traffic / total_calls) * 100 if total_calls else 0,
            'pucar-15_count': emergency_15,
            'pucar-15': (emergency_15 / total_calls) * 100 if total_calls else 0,
            # 'total_generated_cases': round(total_calls - (0.474 * total_calls), 0),
            'total_generated_cases': generated_cases,
            'fir_rate': configs.FIR_RATE * 100,
            'total_fir': int(configs.FIR_RATE * generated_cases) if generated_cases else 0,
            'video_calls': video_calls,
            'rural_response_time': f"{int(regional_avg_response[i * 2][2] // 60)}:{int(regional_avg_response[i * 2][2] % 60):02d}" if round(
                regional_avg_response[i * 2][2], 2) else 0,
            'urban_response_time': f"{int(regional_avg_response[(i * 2) + 1][2] // 60)}:{int(regional_avg_response[(i * 2) + 1][2] % 60):02d}" if round(
                regional_avg_response[(i * 2) + 1][2], 2) else 0,
            'estimated_response_time': f"{int(response_time[i][1] // 60)}:{int(response_time[i][1] % 60):02d}",
            'conference_calls': conference_calls,
            'vccs': vccs,
            'vcm': vcm,
            "response_time_stats": {
                'below_60': response_time_stats[i][0],
                'between_60_5min': response_time_stats[i][1],
                'between_5min_10min': response_time_stats[i][2],
                'between_10min_30min': response_time_stats[i][3],
                'between_30min_60min': response_time_stats[i][4],
                'Above_60min': response_time_stats[i][5]
            }
        }
        response_data.append(stats)
    return response_data


"""GET MAXIMUM TIME OF RECORD IN LEADS_IN"""


def get_max_lead_time():
    connection = sqlite3.connect(configs.PROCESSING_DATA_MASTER)
    cursor = connection.cursor()
    cursor.execute(f'SELECT MAX(time_id) FROM {configs.LEADS_IN_TABLE};')
    result = cursor.fetchone()
    cursor.close()
    connection.close()
    return result[0] if result else None


"""DETAILS OF DISTRICT CASES WITH RESPONSE TIME"""


def get_district_cases(district_id, fromDate, toDate, shift):
    conn = get_db_connection()
    cursor = conn.cursor()

    toDate_dt = datetime.strptime(toDate, "%Y-%m-%d") + timedelta(days=1)
    toDate = toDate_dt.strftime("%Y-%m-%d")

    if shift is not None and shift == configs.DAY_SHIFT:
        # Added parent_id=0 clause because Reponse time is Null for merged_cases

        start_time_1 = f"{fromDate} 08:00:00"
        end_time_1 = f"{fromDate} 19:59:59"
        query = f"""
            SELECT
                case_number,
                district_id,
                datetime(time_id, 'unixepoch', 'localtime') as call_time,
                accepted_time as accepted_time,
                datetime(first_arrival_time, 'unixepoch', 'localtime') as first_arrival_time,
                created_time,
                accept_time,
                start_time,
                responder_id,
                tab,
                datetime(reached_time, 'unixepoch', 'localtime') as reached_time,
                completed_time,
                response_time
            FROM
                response_time
            WHERE
                district_id = ?
                AND parent_id = 0
                AND ((datetime(time_id, 'unixepoch','localtime') BETWEEN ? AND ?))
                AND parent_id = 0
        """
        cursor.execute(query, (district_id, start_time_1, end_time_1))
        rows = cursor.fetchall()
        conn.close()

    elif shift is not None and shift == configs.NIGHT_SHIFT:
        start_time_1 = f"{fromDate} 20:00:00"
        end_time_1 = f"{fromDate} 23:59:59"
        start_time_2 = f"{toDate} 00:00:00"
        end_time_2 = f"{toDate} 08:00:00"
        query = f"""
                    SELECT
                        case_number,
                        district_id,
                        datetime(time_id, 'unixepoch', 'localtime') as call_time,
                        accepted_time as accepted_time,
                        datetime(first_arrival_time, 'unixepoch', 'localtime') as first_arrival_time,
                        created_time,
                        accept_time,
                        start_time,
                        responder_id,
                        tab,
                        datetime(reached_time, 'unixepoch', 'localtime') as reached_time,
                        completed_time,
                        response_time
                    FROM
                        response_time
                    WHERE
                        district_id = ?
                        AND parent_id = 0
                        AND ((datetime(time_id, 'unixepoch','localtime') BETWEEN ? AND ?) OR (datetime(time_id, 'unixepoch','localtime') BETWEEN ? AND ?))
                        AND parent_id = 0
                """
        cursor.execute(query, (district_id, start_time_1, end_time_1, start_time_2, end_time_2))
        rows = cursor.fetchall()
        conn.close()

    else:
        query = f"""
            SELECT
                case_number,
                district_id,
                datetime(time_id, 'unixepoch', 'localtime') as call_time,
                accepted_time as accepted_time,
                datetime(first_arrival_time, 'unixepoch', 'localtime') as first_arrival_time,
                created_time,
                accept_time,
                start_time,
                responder_id,
                tab,
                datetime(reached_time, 'unixepoch', 'localtime') as reached_time,
                completed_time,
                response_time
            FROM
                response_time
            WHERE
                district_id = ?
                AND parent_id = 0
                AND (DATE(datetime(time_id, 'unixepoch','localtime')) = ? ) 
"""
        cursor.execute(query, (district_id, fromDate))
        rows = cursor.fetchall()
        conn.close()

    response = []
    for row in rows:
        case_number, district_id, call_time, accepted_time, first_arrival_time, created_time, accept_time, start_time, responder_id, assigned_by, reached_time, completed_time, response_time = row
        response_time = int(response_time) if response_time else 0
        district = configs.DISTRICTS_DICTIONARY.get(district_id, "Unknown District")  # Default to "Unknown District"
        response.append({
            'case_number': case_number,
            'district': district,
            'call_time': call_time,
            'accepted_time': accepted_time,
            'first_arrival_time': first_arrival_time,
            'created_time': created_time,
            'accept_time': accept_time,
            'start_time': start_time,
            'responder_id': responder_id,
            'assigned_by': assigned_by,
            'reached_time': reached_time,
            'completed_time': completed_time,
            'response_time': f"{int(response_time // 60)}:{int(response_time % 60):02d}"
        })

    return jsonify(response)


def log_to_database(log_conn, log_cursor, level, message):
    query = "INSERT INTO 15_stats_log (status, time_date, description,Host_IP_address) VALUES (%s, %s, %s, %s)"
    data = (level, get_current_time(), message, SYS_IP)
    try:
        log_cursor.execute(query, data)
        # Print the error to the console
        print("ERROR:", message)

        log_conn.commit()
    except mysql.connector.Error as err:
        print(f"Database Error: {err}")

def log_to_database_updated(log_conn, level, message):
    query = """
        INSERT INTO 15_stats_log (status, time_date, description, Host_IP_address)
        VALUES (%s, %s, %s, %s)
    """
    data = (level, get_current_time(), message, SYS_IP)

    try:
        # Execute the INSERT query in a single command
        log_conn.execute(query, data)

        # Print the error to the console
        print("ERROR:", message)

        # Commit the transaction
        log_conn.commit()
    except Exception as err:
        print(f"Database Error: {err}")

def get_current_time():
    return datetime.now().strftime(configs.YMD_HMS)


def district_categorical_response_data(results):
    """Formats the database results into the required response structure."""
    response_data_dict = {}

    for row in results:
        district_id, case_nature, region_category, avg_response_time = row
        key = (district_id, case_nature)

        if key not in response_data_dict:
            response_data_dict[key] = {
                "district_id": configs.DISTRICTS_DICTIONARY[district_id],
                "case_nature": case_nature
            }

        response_data_dict[key][region_category] = f"{int(avg_response_time // 60)}:{int(avg_response_time % 60):02d}"

    return list(response_data_dict.values())


# Processed Data Query
def build_processed_data_query(columns, additional_conditions=None, group_by=None):
    column_str = ", ".join(
        f"{col}" if col in ["district_id", "date"]
        else f"AVG({col})" if col == "estimated_response_time"
        else f"SUM({col})"
        for col in columns
    )

    conditions = []
    if additional_conditions:
        conditions.extend(additional_conditions)

    condition_str = " ".join(conditions)

    group_by_str = f"GROUP BY {group_by}" if group_by else ""

    query = configs.BASE_PROCESSED_DATA_QUERY.format(
        columns=column_str,
        additional_conditions=condition_str
    )
    if group_by_str:
        query += " " + group_by_str

    return query


def build_avg_regional_response_query(columns_key="basic", additional_conditions=None, group_by_key="basic"):
    columns = ", ".join(configs.REGIONAL_RESPONSE_TIME_COLUMNS[columns_key])

    conditions = configs.REGIONAL_RESPONSE_COMMON_CONDITIONS.copy()
    if additional_conditions:
        conditions.extend(additional_conditions)
    conditions_str = " AND ".join(conditions)

    group_by = ", ".join(configs.REGIONAL_RESPONSE_TIME_GROUP_BY[group_by_key])

    query = configs.REGIONAL_RESPONSE_TIME_AVG_BASE_QUERY.format(
        columns=columns,
        conditions=conditions_str
    )

    if group_by:
        query += f" GROUP BY {group_by}"

    return query


def build_agent_stats_query(columns=None, conditions=None, group_by=None, order_by=None):
    if columns is None:
        columns = list(configs.AGENT_STATS_COMMON_COLUMNS.keys())

    column_str = ", ".join(configs.AGENT_STATS_COMMON_COLUMNS[col] for col in columns)

    if conditions is None or not conditions:
        condition_str = ""
    else:
        condition_str = " WHERE " + " AND ".join(configs.AGENT_STATS_CONDITIONS[cond] for cond in conditions)

    group_by_str = f"GROUP BY {', '.join(group_by)}" if group_by else ""
    order_by_str = f"ORDER BY {', '.join(order_by)}" if order_by else ""

    query = configs.AGENT_STATS_BASE_QUERY.format(
        column_str=column_str,
        condition_str=condition_str,
        group_by_str=group_by_str,
        order_by_str=order_by_str
    )
    return query


def build_response_time_query(columns_key="basic", additional_conditions=None, group_by_key=None):
    # Generate columns string
    columns = ", ".join(configs.RESPONSE_TIME_COLUMNS[columns_key])

    conditions = configs.RESPONSE_TIME_COMMON_CONDITIONS.copy()
    if additional_conditions:
        conditions.extend(additional_conditions)
    conditions_str = " AND ".join(conditions)

    group_by = configs.RESPONSE_TIME_GROUP_BY.get(group_by_key, "")
    group_by_str = group_by if group_by else ""

    query = configs.RESPONSE_TIME_BASE_QUERY.format(
        columns=columns,
        conditions=conditions_str,
        group_by=group_by_str
    )

    return query


def build_response_time_stats_query(additional_conditions=None, group_by=None):
    # time range counts
    time_range_counts = []
    for i, (upper_bound, column_name) in enumerate(configs.RESPONSE_TIME_STATS_RANGES):
        lower_bound = configs.RESPONSE_TIME_STATS_RANGES[i - 1][0] if i > 0 else 0
        condition = f"response_time >= {lower_bound} AND response_time < {upper_bound}" if upper_bound != float(
            'inf') else f"response_time > {lower_bound}"
        time_range_counts.append(f"COUNT(CASE WHEN {condition} THEN 1 END) AS {column_name}")

    time_range_counts_str = ",\n        ".join(time_range_counts)

    conditions = []
    if additional_conditions:
        conditions.extend(additional_conditions)
    conditions_str = " AND ".join(conditions)

    group_by_str = f"GROUP BY {group_by}" if group_by else ""

    query = configs.RESPONSE_TIME_STATS_QUERY.format(
        time_range_counts=time_range_counts_str,
        conditions=conditions_str,
        group_by=group_by_str
    )

    return query


def split_name(string):
    try:
        new_str = string.split('@')[0].split('.')
        return str(new_str[0] + " " + new_str[1]).upper()
    except:
        new_str = string.split('.')
        return str(new_str[0] + " " + new_str[1]).upper()


###########################################################
# PostgreSQL Database Connection Utility Functions
###########################################################
def get_processed_db_connection(database=configs.POSTGRES_PROCESSED_STATS_MAIN):
    try:
        conn = psycopg2.connect(
            dbname=database['dbname'],
            user=database['user'],
            password=database['password'],
            host=database['host'],
            port=database['port']
        )
        return conn
    except Exception as e:
        print(e)
        raise


def get_new_processed_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('PROCESSED_DB_NAME'),
            user=os.getenv('PROCESSED_DB_USER'),
            password=os.getenv('PROCESSED_DB_PASSWORD'),
            host=os.getenv('PROCESSED_DB_HOST'),
            port=os.getenv('PROCESSED_DB_PORT')
        )
        return conn
    except Exception as e:
        print(e)
        raise


def log_error_to_db_and_console(db_conn, log_db_cursor, error_message):
    # Capture the error message
    error_message = traceback.format_exc()

    # Log the error to the database
    log_to_database(db_conn, log_db_cursor, "ERROR", error_message)

    # Print the error to the console
    print("ERROR:", error_message)



# def get_category_condition(category):
#     """Returns the SQL condition for the given category."""
#     if category in configs.CRIME_TRENDS_CATEGORIES:
#         return configs.CRIME_TRENDS_CATEGORIES[category]
#     else:
#         return None

def get_category_condition(category):
    """
    Returns the SQL condition for the given category or categories.
    Handles multiple comma-separated categories.
    """
    if category == "":
        # Handle the special case where the category is an empty string
        return configs.CRIME_TRENDS_CATEGORIES.get("", None)

    if not category:
        return None

    # Split the comma-separated categories
    categories = category.split(',')

    # Map each category to its corresponding condition
    conditions = []
    for cat in categories:
        mapped_condition = configs.CRIME_TRENDS_CATEGORIES.get(cat.strip())
        if mapped_condition:
            conditions.append(f"({mapped_condition})")

    # Combine all conditions with OR if there are multiple categories
    if conditions:
        return f" AND ({' OR '.join(conditions)})"
    else:
        return None


def get_week_range(date_str):
    """Calculate start and end of the week (Sunday to Saturday)."""
    given_date = datetime.strptime(date_str, "%Y-%m-%d")
    start_of_week = given_date - timedelta(days=given_date.weekday())
    end_of_week = start_of_week + timedelta(days=6)
    return start_of_week.strftime("%Y-%m-%d"), end_of_week.strftime("%Y-%m-%d")

def get_month_range(date_str):
    """Calculate start and end of the month."""
    start_of_month = datetime.strptime(date_str, "%Y-%m")
    next_month = start_of_month.replace(day=28) + timedelta(days=4)
    end_of_month = next_month - timedelta(days=next_month.day)
    return start_of_month.strftime("%Y-%m-%d"), end_of_month.strftime("%Y-%m-%d")

def get_last_timestamp(remarks):
    try:
        # Safely evaluate the string as a Python object
        dictionaries = json.loads(remarks)
        if isinstance(dictionaries, list) and dictionaries:
            last_dict = dictionaries[-1]
            return last_dict.get('timestamp', None)
        elif isinstance(dictionaries, dict) and dictionaries:
            return dictionaries.get('timestamp', None)
        return None
    except (ValueError, SyntaxError):
        return None


def send_fcm_notification(user_ids, title, body, data=None):
    try:
        processed_db_conn, processed_db_cursor = get_processed_db_connection()
        tokens = []
        for user_id in user_ids:
            processed_db_cursor.execute(
                "SELECT fcm_token FROM user_fcm_tokens WHERE user_name = %s",
                (user_id.strip(),)
            )
            tokens.extend([row[0] for row in processed_db_cursor.fetchall()])
        processed_db_conn.close()

        if not tokens:
            return

        # Send multicast message to all tokens
        message = messaging.MulticastMessage(
            tokens=tokens,
            notification=messaging.Notification(title=title, body=body),
            data=data
        )
        response = messaging.send_each_for_multicast(message)

        # Handle failed tokens
        if response.failure_count > 0:
            for idx, resp in enumerate(response.responses):
                if resp.exception:
                    token = tokens[idx]

    except Exception as e:
        print(e)
        raise