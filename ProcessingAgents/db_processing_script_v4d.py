'''
***MOST LATEST***
Code processing file to INCORPORATE UPDATED Consolidated Dashboards (VCCS,VCM,VWPS) table and there respective Cases
tables. Latest file before this file for processing is db_processing_script_v4c_conf.py
'''
import time
from datetime import datetime, timedelta
from Utilities import configs
from Utilities import utils
from Utilities import db_config
import traceback
from decimal import Decimal

db_conn = db_config.get_db_connection()
log_db_cursor = db_conn.cursor()


def process_date(db_connection, start_timestamp, end_timestamp):
    try:
        cursor = db_connection.cursor()

        queries = {
            'total_calls': """
                    SELECT 
                        district_id, 
                        police_station,
                        HOUR(FROM_UNIXTIME(time_id)) AS hour, 
                        COUNT(*) AS count
                    FROM `15_preprocessed`
                    WHERE
                        time_id BETWEEN %s AND %s
                        AND district_id IS NOT NULL
                        AND status NOT IN ('IVR DROP', 'QUEUE', 'test')
                    GROUP BY district_id, police_station, hour;
                """,
            'counts': """
                    SELECT 
                        district_id,
                        police_station,
                        HOUR(FROM_UNIXTIME(time_id)) AS hour,
                        SUM(CASE WHEN selected_option = 'siraiki-15' THEN 1 ELSE 0 END) AS siraiki_count,
                        SUM(CASE WHEN selected_option = 'punjabi-15' THEN 1 ELSE 0 END) AS punjabi_count,
                        SUM(CASE WHEN selected_option = 'potohari-15' THEN 1 ELSE 0 END) AS potohari_count,
                        SUM(CASE WHEN selected_option = 'female-15' THEN 1 ELSE 0 END) AS vwps_count,
                        SUM(CASE WHEN selected_option = 'traffic-15' THEN 1 ELSE 0 END) AS traffic_count,
                        SUM(CASE WHEN selected_option = 'english-15' THEN 1 ELSE 0 END) AS english_count,
                        SUM(CASE WHEN disconnection_cause = 'TRANSFER' THEN 1 ELSE 0 END) AS transfered_count,
                        SUM(CASE WHEN call_type = 'manual' THEN 1 ELSE 0 END) AS call_backs_count,
                        SUM(CASE WHEN queue = 'child-safety-15' THEN 1 ELSE 0 END) AS vccs_count,
                        SUM(CASE WHEN queue = 'minorities-15' THEN 1 ELSE 0 END) AS vcm_count,
                        SUM(CASE WHEN (status = 'CompCa' AND parent_id = 0) THEN 1 ELSE 0 END) AS generated_cases_count 
                    FROM `15_preprocessed`
                    WHERE 
                        district_id IS NOT NULL
                        AND time_id BETWEEN %s AND %s
                        AND (
                            selected_option IN ('siraiki-15', 'punjabi-15', 'potohari-15', 'female-15', 'traffic-15', 'english-15') 
                            OR disconnection_cause = 'TRANSFER' 
                            OR call_type = 'manual' 
                            OR queue IN ('child-safety-15', 'minorities-15')
                            OR status = 'CompCa'
                        )
                    GROUP BY district_id, police_station, hour;
            """,
            'conference_calls': """
                    SELECT
                        l.district_id,
                        l.police_station,
                        HOUR(FROM_UNIXTIME(l.time_id)) AS hour,                                                                                                                                                                                                                                                                                                                                                                   
                        COUNT(CASE WHEN l.field3 IN ("Successful Conference call") THEN 1 END) AS successful_calls,
                        COUNT(CASE WHEN l.field3 IN ("FO did not attend the call") THEN 1 END) AS unsuccessful_calls
                    FROM 
                        `15_preprocessed` l
                    WHERE 
                        l.district_id IS NOT NULL 
                        AND l.time_id BETWEEN %s AND %s
                        AND l.field3 is NOT NULL
                    GROUP BY 
                        l.district_id, hour
                """
        }

        results = {}

        for key, query in queries.items():
            cursor.execute(query, (start_timestamp, end_timestamp))

            if key == 'counts':
                # Fetch data for all conditional counts in a single query
                for district_id, police_station, hour, siraiki_count, punjabi_count, potohari_count, vwps_count, traffic_count, english_count, transfered_count, call_backs_count, vccs_count, vcm_count, generated_cases_count in cursor.fetchall():
                    if isinstance(district_id, bytes):
                        district_id = district_id.decode('utf-8')

                    if isinstance(police_station, bytes):
                        police_station = police_station.decode('utf-8')

                    if (district_id, police_station, hour) not in results:
                        results[(district_id, police_station, hour)] = {}
                    results[(district_id, police_station, hour)].update({
                        'siraiki': int(siraiki_count) if isinstance(siraiki_count, Decimal) else siraiki_count,
                        'punjabi': int(punjabi_count) if isinstance(punjabi_count, Decimal) else punjabi_count,
                        'potohari': int(potohari_count) if isinstance(potohari_count, Decimal) else potohari_count,
                        'vwps': int(vwps_count) if isinstance(vwps_count, Decimal) else vwps_count,
                        'traffic': int(traffic_count) if isinstance(traffic_count, Decimal) else traffic_count,
                        'english': int(english_count) if isinstance(english_count, Decimal) else english_count,
                        'transfered': int(transfered_count) if isinstance(transfered_count,
                                                                          Decimal) else transfered_count,
                        'call_backs': int(call_backs_count) if isinstance(call_backs_count,
                                                                          Decimal) else call_backs_count,
                        'vccs': int(vccs_count) if isinstance(vccs_count, Decimal) else vccs_count,
                        'vcm': int(vcm_count) if isinstance(vcm_count, Decimal) else vcm_count,
                        'generated_cases': int(generated_cases_count) if isinstance(generated_cases_count,
                                                                                    Decimal) else generated_cases_count
                    })

            elif key == 'total_calls':
                for district_id, police_station, hour, count in cursor.fetchall():
                    if isinstance(district_id, bytes):
                        district_id = district_id.decode('utf-8')
                    if isinstance(police_station, bytes):
                        police_station = police_station.decode('utf-8')
                    if (district_id, police_station, hour) not in results:
                        results[(district_id, police_station, hour)] = {}
                    results[(district_id, police_station, hour)]['total_calls'] = count

            elif key == 'conference_calls':
                for district_id, police_station, hour, successful_calls, unsuccessfull_calls in cursor.fetchall():

                    if isinstance(district_id, bytes):
                        district_id = district_id.decode('utf-8')
                    if isinstance(police_station, bytes):
                        police_station = police_station.decode('utf-8')

                    if (district_id, police_station, hour) not in results:
                        results[(district_id, police_station, hour)] = {}

                    results[(district_id, police_station, hour)]['succ_conf_calls'] = successful_calls
                    results[(district_id, police_station, hour)]['unsucc_conf_calls'] = unsuccessfull_calls

        queries_without_district = {
            'app_alerts': """
                    SELECT 
                        HOUR(FROM_UNIXTIME(time_id)) AS hour, 
                        COUNT(*) AS count  
                    FROM `women_safety_api_logs_preprocessed`
                    WHERE time_id BETWEEN %s AND %s 
                    GROUP BY hour;
                """
        }
        PS_WOMEN_SAFETY = "NULL"
        for key, query_without_district in queries_without_district.items():
            cursor.execute(query_without_district, (start_timestamp, end_timestamp,))
            for hour, count in cursor.fetchall():
                if (configs.DEFAULT_DISTRICT_ID, PS_WOMEN_SAFETY, hour) not in results:
                    results[(configs.DEFAULT_DISTRICT_ID, PS_WOMEN_SAFETY, hour)] = {}
                results[(configs.DEFAULT_DISTRICT_ID, PS_WOMEN_SAFETY, hour)][key] = count

        cursor.execute(
            "SELECT HOUR(created_at) AS hour, COUNT(*)  FROM `video_calls_preprocessed` WHERE created_at BETWEEN %s AND %s GROUP BY hour",
            (str(datetime.fromtimestamp(start_timestamp)), str(datetime.fromtimestamp(end_timestamp)))
        )
        for hour, count in cursor.fetchall():
            if (configs.DEFAULT_DISTRICT_ID, hour) not in results:
                results[(configs.DEFAULT_DISTRICT_ID, hour)] = {}
            results[(configs.DEFAULT_DISTRICT_ID, hour)]['video_calls'] = count
        return results
    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def insert_results(db_connection, results, date):
    try:
        cursor = db_connection.cursor()
        insert_query = """
        INSERT OR REPLACE INTO processed_data (
            date, hour, district_id,police_station, total_calls,generated_cases, siraiki, punjabi, potohari, vwps, traffic, english, app_alerts, 
            transfered, call_backs, avg_response_time, estimated_response_time, 
            video_calls,succ_conf_calls,unsucc_conf_calls, vccs, vcm
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?)
        """

        # Define default values for each column
        default_values = {
            'total_calls': 0,
            'generated_cases': 0,
            'siraiki': 0,
            'punjabi': 0,
            'potohari': 0,
            'vwps': 0,
            'traffic': 0,
            'english': 0,
            'app_alerts': 0,
            'transfered': 0,
            'call_backs': 0,
            'avg_response_time': 37,  # or another default value if applicable
            'estimated_response_time': 30,  # or another default value if applicable
            'video_calls': 0,
            'succ_conf_calls': 0,
            'unsucc_conf_calls': 0,
            'vccs': 0,
            'vcm': 0
        }

        for (district_id, police_station, hour), result in results.items():
            row = {
                'date': date,
                'hour': f"{hour}",
                'district_id': district_id,
                'police_station': police_station
            }
            row.update(default_values)
            row.update(result)

            cursor.execute(insert_query, (
                row['date'], row['hour'], row['district_id'],
                row['police_station'] if row['police_station'] is not None else 'UNKNOWN', row['total_calls'],
                row['generated_cases'], row['siraiki'], row['punjabi'], row['potohari'], row['vwps'], row['traffic'],
                row['english'], row['app_alerts'], row['transfered'], row['call_backs'], row['avg_response_time'],
                row['estimated_response_time'], row['video_calls'], row['succ_conf_calls'], row['unsucc_conf_calls'],
                row['vccs'], row['vcm']
            ))

        db_connection.commit()
    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def response_time(primary_conn, processed_conn, start_timestamp, end_timestamp):
    try:
        processed_cursor = processed_conn.cursor()
        processed_cursor.execute("""
        CREATE TABLE IF NOT EXISTS response_time (
            date TEXT,
            lead_id INTEGER UNIQUE,
            parent_id INTEGER,
            time_id TEXT,
            case_number TEXT,
            description TEXT,
            response_time INTEGER,
            responder_id INTEGER,
            accept_time TEXT,
            accepted_time TEXT,
            reached_time TEXT,
            first_arrival_time TEXT,
            dispatched_time TEXT,
            start_time TEXT,
            police_station_id INTEGER,
            police_station TEXT,
            police_circle INTEGER,
            queue TEXT,
            district_id INTEGER,
            region_category TEXT,
            created_time TEXT,
            tab TEXT,
            completed_time TEXT,
            caller_name TEXT,
            caller_number TEXT,
            job_status TEXT,
            caller_location TEXT,
            complete_by TEXT,
            level1_case_nature TEXT,
            level2_case_nature TEXT,
            level3_case_nature TEXT,
            responder_lat TEXT,
            responder_long TEXT,
            field3 TEXT,
            lat TEXT,
            long TEXT,
            remarks TEXT,
            assignedby_remarks TEXT,
            remarks_status TEXT,
            assignedto_remarks TEXT
        )
        """)
        processed_conn.commit()

        primary_cursor = primary_conn.cursor()

        query = """
        SELECT 
            DATE(FROM_UNIXTIME(l.time_id)) AS date,
            l.lead_id,
            l.parent_id,
            l.time_id,
            l.case_number,
            l.cro_comments,
            CASE 
                WHEN (d.reached_time IS NULL OR (UNIX_TIMESTAMP(d.reached_time) < l.accepted_time)) 
                     AND l.first_arrival_time <> 0 
                     AND l.first_arrival_time IS NOT NULL THEN 
                        l.first_arrival_time - l.accepted_time
                WHEN d.reached_time IS NOT NULL AND (UNIX_TIMESTAMP(d.reached_time) > l.accepted_time) THEN 
                        UNIX_TIMESTAMP(d.reached_time) - l.accepted_time
                ELSE 
                    NULL
            END AS response_time,
            d.responder_id,
            MAX(d.accept_time) AS accept_time,
            FROM_UNIXTIME(l.accepted_time) AS accepted_time,
            MAX(UNIX_TIMESTAMP(d.reached_time)) AS reached_time,
            l.first_arrival_time,
            l.dispatched_time,
            l.police_station_id,
            l.police_station,
            l.police_circle,
            l.queue,
            l.district_id AS district_id,
            MAX(d.job_status) AS job_status,
            l.caller_location AS caller_location,
            MAX(d.created_time) AS created_time,
            CASE 
                WHEN d.created_time IS NULL THEN 'Manual'
                WHEN d.created_time < FROM_UNIXTIME(l.accepted_time) THEN 'Agent'
                ELSE 'District'
            END AS tab,
            MAX(d.completed_time) AS completed_time,
            MAX(d.start_time) AS start_time,
            MAX(d.complete_by) AS complete_by,
            l.caller_name AS name,
            l.cli,
            p.region_category,
            l.level1_case_nature,
            l.level2_case_nature,
            l.level3_case_nature,
            MAX(d.reached_lat),
            MAX(d.reached_long),
            l.field3,
            l.lat,
            l.long
        FROM 
            `15_preprocessed` l
        LEFT JOIN 
            `duties_preprocessed` d ON l.lead_id = d.lead_id
        LEFT JOIN 
            `15_police_stations` p ON l.police_station_id = p.id
        WHERE 
            l.status = 'CompCa' AND
            l.time_id BETWEEN %s AND %s
        GROUP BY 
            l.lead_id, l.time_id, l.case_number, l.first_arrival_time, l.cli, 
            l.district_id, l.police_station_id,
            l.accepted_time, l.level1_case_nature, l.level2_case_nature, l.level3_case_nature, l.field3;
        """

        primary_cursor.execute(query, (start_timestamp, end_timestamp))
        rows = primary_cursor.fetchall()

        processed_results = []

        for row in rows:
            processed_row = []
            for col in row:
                if isinstance(col, bytes):
                    try:
                        processed_row.append(col.decode('utf-8'))
                    except UnicodeDecodeError:
                        processed_row.append(int(col))
                elif isinstance(col, (datetime)):
                    formatted_date = col.strftime('%d-%m-%Y %H:%M:%S')
                    processed_row.append(formatted_date)
                elif isinstance(col, Decimal):
                    processed_row.append(float(col))
                else:
                    processed_row.append(col)
            processed_results.append(processed_row)

        for processed_row in processed_results:
            processed_cursor.execute("""
                        INSERT OR REPLACE INTO response_time (
                            date, lead_id, parent_id, time_id, case_number, description, response_time,
                            responder_id, accept_time, accepted_time, reached_time, first_arrival_time,
                            dispatched_time,police_station_id, police_station,police_circle, queue, district_id, job_status, 
                            caller_location,created_time, tab, completed_time, start_time, complete_by, caller_name, 
                            caller_number,region_category, level1_case_nature, level2_case_nature, level3_case_nature,responder_lat,responder_long,field3,lat,long
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, processed_row)
        processed_conn.commit()

    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def process_punjab_today(db_connection, start_timestamp, end_timestamp):
    try:
        cursor = db_connection.cursor()

        # Query for murder and related cases
        category_query = """
                SELECT 
                district_id,
                police_station,
                COUNT(*) AS count,
                CASE 
                    WHEN level3_case_nature IN ('Murder','Attempt to Murder') THEN 'murder'
                    WHEN level3_case_nature IN ('Sexual Assault/ Harrasment To Women') THEN 'rape'
                    WHEN level3_case_nature IN ('Rape','Child Abuse / Molestation') THEN 'child_abuse'
                    WHEN level3_case_nature = 'Aerial Firing' THEN 'aerial_firing'
                    WHEN level3_case_nature in ('Male Kidnapping/ Abduction','Female Kidnapping/ Abduction', 'Child Kidnapping' , 
                        'Attempt to Kidnap / Abduct' , 'Kidnapping for Ransom' , 'Child Kidnapping ') THEN 'kidnapping'
                    WHEN level3_case_nature in ('Highway/Road/Street Robbery','Any Other Robbery', 'Cattle Robbery',
                            'House Robbery', 'Shop Robbery','Patrol Pump Robbery','Bank/Money Exchange/ ATM Robbery','Car Snatching',
                            'Other Vehicles Snatching','Snatching/Jhapatta','Motorcycle Snatching','Jewellery Shop Robbery','Robbery with Murder')
                    THEN 'robbery_snatching'
                    WHEN level3_case_nature = 'Dacoity with Murder' THEN 'dacoity_with_murder'
                    WHEN level3_case_nature in ('Highway/Road/Street Dacoity',
                            'House Dacoity' ,'Any Other Dacoity' , 'Shop Dacoity' ,'Cattle Dacoity','Patrol Pump Dacoity','Jewellery Shop Dacoity')
                    THEN 'dacoity'
                    WHEN level3_case_nature = 'Motorcycle Theft' THEN 'motorcycle_theft'
                    WHEN level3_case_nature IN ('Mobile Theft','Any Other Theft','Cattle theft',
                    'Transformer/ Motor Theft','Pick Pocketing','Purse / Wallet / Luggage Theft',
                    'Cycle Theft','Weapon Theft','Other Vehicles Theft','Other Burglary','Shop Burglary','House Burglary') THEN 'theft'
                    WHEN level3_case_nature in ('Other Burglary','Shop Burglary','House Burglary') THEN 'burglary'
                    WHEN level3_case_nature = 'Car Theft' THEN 'car_theft'
                    WHEN level3_case_nature in ('Firing on Police','Suicidal Attack/ Bomb Blast/ Terrorist Attack') THEN 'terrorist_act'
                    WHEN level3_case_nature in ('Prostitution/ Brothel House','Acid Throwing','Hurt / Injuries','Street Fight',
                            'Other Assault','Physical Threats / Harrasment','Domestic Violence','Criminal Intimidation (Threat with Weapon)')
                    THEN 'other_person'
                    WHEN level2_case_nature = 'Assault / Hurt' THEN 'hurt'
                    WHEN level3_case_nature = 'Attempt to Illegal Possession of Land/ Premises' THEN 'other_property'
                    ELSE 'other'
                END AS case_type
            FROM `15_preprocessed`
            WHERE time_id BETWEEN %s AND %s
              AND status = 'CompCa'
              AND district_id IS NOT NULL
              AND police_station IS NOT NULL
              AND parent_id = 0
            GROUP BY district_id, police_station, case_type;
            """

        # Execute the murder query
        cursor.execute(category_query, (start_timestamp, end_timestamp))
        murder_results = cursor.fetchall()

        # Query for minorities
        query_minorities = """
                SELECT district_id, police_station, COUNT(*) as count
                FROM `15_preprocessed`
                WHERE time_id BETWEEN %s AND %s
                AND queue = 'minorities-15'
                AND status = 'CompCa'
                AND district_id is Not Null
                AND police_station is not Null
                AND parent_id = 0
                GROUP BY district_id, police_station;
            """

        # Execute the minorities query
        cursor.execute(query_minorities, (start_timestamp, end_timestamp))
        minorities_results = cursor.fetchall()

        # Prepare results for insertion into insert_punjab_today
        results = {}

        for district_id, police_station, count, case_type in murder_results:
            if isinstance(district_id, bytes):
                district_id = district_id.decode('utf-8')

            if isinstance(police_station, bytes):
                police_station = police_station.decode('utf-8')

            if (district_id, police_station) not in results:
                results[(district_id, police_station)] = {
                    'murder': 0,
                    'rape': 0,
                    'child_abuse': 0,
                    'aerial_firing': 0,
                    'kidnapping': 0,
                    'minorities': 0,
                    'robbery_snatching': 0,
                    'dacoity': 0,
                    'motorcycle_theft': 0,
                    'burglary': 0,
                    'car_theft': 0,
                    'theft': 0,
                    'terrorist_act': 0,
                    'other_person': 0,
                    'other_property': 0,
                    'hurt': 0,
                    'dacoity_with_murder': 0,
                    'other': 0
                }
            results[(district_id, police_station)][case_type] += count

        for district_id, police_station, count in minorities_results:
            if isinstance(district_id, bytes):
                district_id = district_id.decode('utf-8')

            if isinstance(police_station, bytes):
                police_station = police_station.decode('utf-8')

            if (district_id, police_station) not in results:
                results[(district_id, police_station)] = {
                    'murder': 0,
                    'rape': 0,
                    'child_abuse': 0,
                    'aerial_firing': 0,
                    'kidnapping': 0,
                    'minorities': 0,
                    'robbery_snatching': 0,
                    'dacoity': 0,
                    'motorcycle_theft': 0,
                    'burglary': 0,
                    'car_theft': 0,
                    'theft': 0,
                    'terrorist_act': 0,
                    'other_person': 0,
                    'other_property': 0,
                    'hurt': 0,
                    'dacoity_with_murder': 0
                }
            results[(district_id, police_station)]['minorities'] += count
        return results

    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def insert_punjab_today(db_connection, results, date):
    try:
        cursor = db_connection.cursor()
        insert_query = """
        INSERT OR REPLACE INTO punjab_today (
            date, police_station, district_id, murder,minorities,hurt,burglary, aerial_firing, rape, kidnapping, robbery_snatching, 
            dacoity_with_murder,dacoity, motorcycle_theft, car_theft, theft, child_abuse,
            terrorist_act, other_person, other_property
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?,?,?,?)
        """

        # Define default values for each column
        default_values = {
            'murder': 0,
            'aerial_firing': 0,
            'rape': 0,
            'kidnapping': 0,
            'robbery_snatching': 0,
            'dacoity': 0,
            'motorcycle_theft': 0,
            'car_theft': 0,
            'theft': 0,
            'terrorist_act': 0,
            'other_person': 0,
            'other_property': 0,
            'child_abuse': 0,
            'minorities': 0,
            'hurt': 0,
            'burglary': 0,
            'dacoity_with_murder': 0
        }

        for (district_id, police_station), result in results.items():
            row = {
                'date': date,
                'police_station': f"{police_station}",
                'district_id': district_id
            }
            row.update(default_values)
            row.update(result)

            cursor.execute(insert_query, (
                row['date'], row['police_station'], row['district_id'], row['murder'], row['minorities'], row['hurt'],
                row['burglary'], row['aerial_firing'], row['rape'], row['kidnapping'],
                row['robbery_snatching'], row['dacoity_with_murder'], row['dacoity'], row['motorcycle_theft'],
                row['car_theft'],
                row['theft'], row['child_abuse'], row['terrorist_act'], row['other_person'], row['other_property']
            ))

        db_connection.commit()
    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def process_fir_cases(db_conn,processed_conn, start_timestamp, end_timestamp,date):
    try:
        cursor = db_conn.cursor()
        processed_cursor = processed_conn.cursor()

        query = """
        WITH categorized_cases AS (
            SELECT 
                CASE 
                    WHEN level3_case_nature IN ('Male Kidnapping/ Abduction', 'Female Kidnapping/ Abduction', 
                                              'Child Kidnapping', 'Attempt to Kidnap / Abduct', 
                                              'Kidnapping for Ransom', 'Child Kidnapping ') 
                    THEN 'kidnapping'
                    WHEN level3_case_nature IN ('Highway/Road/Street Robbery','Any Other Robbery', 'Cattle Robbery',
                                              'House Robbery', 'Shop Robbery','Patrol Pump Robbery','Bank/Money Exchange/ ATM Robbery',
                                              'Car Snatching', 'Other Vehicles Snatching','Snatching/Jhapatta',
                                              'Motorcycle Snatching','Jewellery Shop Robbery')
                    THEN 'robbery_snatching'
                    WHEN level3_case_nature IN ('Mobile Theft','Any Other Theft','Cattle theft',
                                              'Transformer/ Motor Theft','Pick Pocketing','Purse / Wallet / Luggage Theft',
                                              'Cycle Theft','Weapon Theft')
                    THEN 'theft'
                    WHEN queue = 'minorities-15' THEN 'minorities'
                    WHEN level3_case_nature IN ('Highway/Road/Street Dacoity','House Dacoity' ,'Any Other Dacoity' , 
                                              'Shop Dacoity' ,'Cattle Dacoity', 'Patrol Pump Dacoity','Jewellery Shop Dacoity') 
                    THEN 'dacoity'
                    WHEN level3_case_nature IN ('Dacoity with Murder') THEN 'dacoity_with_murder'
                    WHEN level3_case_nature IN ('Rape','Sexual Assault/ Harrasment To Women') THEN 'rape'
                    WHEN level3_case_nature IN ('Firing on Police','Suicidal Attack/ Bomb Blast/ Terrorist Attack') 
                    THEN 'terrorist_act'
                    WHEN level3_case_nature IN ('Child Abuse / Molestation') THEN 'child_abuse'
                    WHEN level3_case_nature IN ('Murder','Attempt to Murder') THEN 'murder'
                    WHEN level3_case_nature IN ('Car Theft') THEN 'car_theft'
                    WHEN level3_case_nature = 'Aerial Firing' THEN 'aerial_firing'
                    WHEN level3_case_nature = 'Motorcycle Theft' THEN 'motorcycle_theft'
                    WHEN level3_case_nature IN ('House Burglary','Shop Burglary','Other Burglary','Bank Burglary') 
                    THEN 'burglary'
                    WHEN level3_case_nature IN ('Prostitution/ Brothel House','Acid Throwing','Hurt / Injuries',
                                              'Street Fight','Other Assault', 'Physical Threats / Harrasment',
                                              'Domestic Violence','Criminal Intimidation (Threat with Weapon)')
                    THEN 'other_person'
                    WHEN level3_case_nature IN ('Attempt to Illegal Possession of Land/ Premises') 
                    THEN 'other_property'
                END as category,
                lead_id,district_id,police_station
            FROM 15_preprocessed
            WHERE time_id BETWEEN %s AND %s
        )
        SELECT 
            c.category,
            c.district_id,
            c.police_station,
            COUNT(DISTINCT f.lead_id) as fir_count
        FROM categorized_cases c
        LEFT JOIN leads_in_fir_preprocessed f ON c.lead_id = f.lead_id 
        WHERE c.category IS NOT NULL
            AND c.police_station IS NOT NULL
            AND c.district_id IS NOT NULL
        GROUP BY c.district_id, c.police_station, c.category;
        """

        cursor.execute(query, (start_timestamp,end_timestamp))
        results = cursor.fetchall()

        fir_data_template = {
            'murder': 0, 'dacoity': 0, 'aerial_firing': 0, 'rape': 0,
            'minorities': 0, 'hurt': 0, 'burglary': 0, 'kidnapping': 0,
            'child_abuse': 0, 'robbery_snatching': 0, 'motorcycle_theft': 0,
            'dacoity_with_murder': 0, 'car_theft': 0, 'theft': 0,
            'terrorist_act': 0, 'other_person': 0, 'other_property': 0
        }

        # Prepare data for each district_id and police_station
        fir_rows = {}
        for category, district_id, police_station, fir_count in results:
            if isinstance(district_id, bytes):
                district_id = district_id.decode('utf-8')
            if isinstance(police_station, bytes):
                police_station = police_station.decode('utf-8')
            key = (district_id, police_station)
            if key not in fir_rows:
                fir_rows[key] = {**fir_data_template, 'district_id': district_id, 'police_station': police_station,
                                 'date': date}
            fir_rows[key][category] = fir_count

        # Move insertion logic outside the loop
        insert_query = """
            INSERT OR REPLACE INTO fir_cases (
                date, district_id, police_station, murder, dacoity, aerial_firing, rape, minorities, hurt, burglary,
                kidnapping, child_abuse, robbery_snatching, motorcycle_theft, dacoity_with_murder, car_theft, theft,
                terrorist_act, other_person, other_property
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for row in fir_rows.values():
            row_values = (
                row['date'], row['district_id'], row['police_station'], row['murder'], row['dacoity'],
                row['aerial_firing'], row['rape'], row['minorities'], row['hurt'], row['burglary'],
                row['kidnapping'], row['child_abuse'], row['robbery_snatching'], row['motorcycle_theft'],
                row['dacoity_with_murder'], row['car_theft'], row['theft'], row['terrorist_act'],
                row['other_person'], row['other_property']
            )
            processed_cursor.execute(insert_query, row_values)
            processed_conn.commit()

    except Exception as e:
        utils.log_to_database(processed_conn, None, "ERROR", traceback.format_exc())


def process_combined_dashboard(vccs_conn, vwps_conn, vcm_conn, start_timestamp, end_timestamp):
    results = {}

    # Define queries and connections
    db_sources = {
        'vccs': {
            'conn': vccs_conn,
            'query': """
                        SELECT
                            district_id,
                            pucar_police_station,
                            COUNT(*) AS total_vccs,
                            SUM(CASE WHEN final_status_id = 8 THEN 1 ELSE 0 END +
                                CASE WHEN created_at >= %s AND created_at <= %s AND final_status_id = 12 
                                AND (handed_over_to IN (1, 3, 4, 5)) THEN 1 ELSE 0 END) AS under_inquiry_vccs,
                            COUNT(CASE WHEN final_status_id = 7 THEN 1 ELSE NULL END) AS escalated_vccs,
                            COUNT(CASE WHEN is_fir_registered = 1 OR is_challan_submitted = 1 THEN 1 ELSE NULL END) AS fir_vccs,
                            COUNT(CASE WHEN is_challan_submitted = 1 THEN 1 ELSE NULL END) AS challan_vccs,
                            SUM(CASE WHEN final_status_id = 6 THEN 1 ELSE 0 END +
                                CASE WHEN final_status_id = 12 AND handed_over_to = 2 AND created_at >= %s AND created_at <= %s THEN 1 ELSE 0 END) AS resolved_vccs
                        FROM case_final_status
                        WHERE created_at BETWEEN %s AND %s
                            AND district_id IS NOT NULL
                        GROUP BY district_id, pucar_police_station
                    """,
            'params': (start_timestamp, end_timestamp, start_timestamp, end_timestamp, start_timestamp, end_timestamp)
        },
        'vwps': {
            'conn': vwps_conn,
            'query': """
                        SELECT
                            district_id,
                            pucar_police_station,
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
                        GROUP BY district_id, pucar_police_station
                    """,
            'params': (start_timestamp, end_timestamp),
        },
        'vcm': {
            'conn': vcm_conn,
            'query': """
                        SELECT
                            district_id,
                            pucar_police_station,
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
                        GROUP BY district_id, pucar_police_station
            """,
            'params': (start_timestamp, end_timestamp)
        }
    }

    # Fetch data from each source
    for key, source in db_sources.items():
        cursor = source['conn'].cursor()
        cursor.execute(source['query'], source['params'])
        for row in cursor.fetchall():
            district_id, police_station, *metrics = row
            if isinstance(district_id, bytes):
                district_id = district_id.decode('utf-8')
            if isinstance(police_station, bytes):
                police_station = police_station.decode('utf-8')

            metrics = [int(c) if isinstance(c, Decimal) and c % 1 == 0 else float(c) if isinstance(c, Decimal) else c
                       for
                       c in metrics]

            district_key = (district_id, police_station)
            if district_key not in results:
                results[district_key] = {}
            results[district_key].update(dict(zip([f"{key}_{metric}" for metric in
                                                      ['total', 'under_inquiry', 'escalated', 'fir', 'challan',
                                                       'resolved']
                                                  ][:len(metrics)], metrics)))

    return results


def insert_combined_dashboard_data(db_conn, data, date):
    """
    Inserts or updates dashboard data into the database.

    Args:
        db_conn: Database connection object.
        data: Dictionary containing dashboard data, where keys are tuples of (district_id, police_station)
              and values are dictionaries of metrics.

    Returns:
        None
    """
    try:
        cursor = db_conn.cursor()
        for (district_id, police_station), metrics in data.items():
            cursor.execute("""
                INSERT INTO combined_dashboards (
                    date, district_id, police_station, 
                    total_vccs, under_inquiry_vccs, escalated_vccs, 
                    fir_vccs, challan_vccs, resolved_vccs, total_vwps,
                    under_inquiry_vwps, escalated_vwps, fir_vwps, challan_vwps,
                    resolved_vwps, total_vcm, under_inquiry_vcm, escalated_vcm,
                    fir_vcm, challan_vcm, resolved_vcm
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, district_id, police_station) DO UPDATE SET
                    total_vccs = excluded.total_vccs,
                    under_inquiry_vccs = excluded.under_inquiry_vccs,
                    escalated_vccs = excluded.escalated_vccs,
                    fir_vccs = excluded.fir_vccs,
                    challan_vccs = excluded.challan_vccs,
                    resolved_vccs = excluded.resolved_vccs,
                    total_vwps = excluded.total_vwps,
                    under_inquiry_vwps = excluded.under_inquiry_vwps,
                    escalated_vwps = excluded.escalated_vwps,
                    fir_vwps = excluded.fir_vwps,
                    challan_vwps = excluded.challan_vwps,
                    resolved_vwps = excluded.resolved_vwps,
                    total_vcm = excluded.total_vcm,
                    under_inquiry_vcm = excluded.under_inquiry_vcm,
                    escalated_vcm = excluded.escalated_vcm,
                    fir_vcm = excluded.fir_vcm,
                    challan_vcm = excluded.challan_vcm,
                    resolved_vcm = excluded.resolved_vcm
            """, (
                date,
                district_id,
                police_station,
                metrics.get('vccs_total', 0),
                metrics.get('vccs_under_inquiry', 0),
                metrics.get('vccs_escalated', 0),
                metrics.get('vccs_fir', 0),
                metrics.get('vccs_challan', 0),
                metrics.get('vccs_resolved', 0),
                metrics.get('vwps_total', 0),
                metrics.get('vwps_under_inquiry', 0),
                metrics.get('vwps_escalated', 0),
                metrics.get('vwps_fir', 0),
                metrics.get('vwps_challan', 0),
                metrics.get('vwps_resolved', 0),
                metrics.get('vcm_total', 0),
                metrics.get('vcm_under_inquiry', 0),
                metrics.get('vcm_escalated', 0),
                metrics.get('vcm_fir', 0),
                metrics.get('vcm_challan', 0),
                metrics.get('vcm_resolved', 0)
            ))
        db_conn.commit()
    except Exception as e:
        print(f"Error inserting data: {e}")
        db_conn.rollback()
    finally:
        cursor.close()


def vccs_records(db_conn, processed_conn, start_timestamp, end_timestamp):
    try:
        processed_cursor = processed_conn.cursor()

        primary_cursor = db_conn.cursor()

        query = """
                SELECT 
                    cfs.lead_id,
                    cfs.pucar_time_id,
                    cfs.pucar_case_number,
                    cfs.pucar_district_id,
                    cfs.pucar_district,
                    cfs.pucar_police_station,
                    cfs.pucar_police_station_id,
                    cfs.level1_case_nature,
                    cfs.pucar_level2_case_nature,
                    cfs.level3_case_nature,
                    cfs.pucar_caller_name,
                    cfs.pucar_cli,
                    cfs.final_status_id,
                    cfs.final_status_remarks,
                    cfs.pucar_cro_comments,
                    cfs.created_at,
                    cfs.handed_over_to,
                    cfs.is_fir_registered,
                    cfs.is_challan_submitted
                FROM 
                    `case_final_status` cfs
                WHERE 
                    cfs.pucar_status = 'CompCa' AND
                    cfs.created_at BETWEEN %s AND %s
                """

        primary_cursor.execute(query, (start_timestamp, end_timestamp))
        rows = primary_cursor.fetchall()

        processed_results = []

        for row in rows:
            processed_row = []
            for col in row:
                if isinstance(col, bytes):
                    try:
                        processed_row.append(col.decode('utf-8'))
                    except UnicodeDecodeError:
                        processed_row.append(int(col))
                elif isinstance(col, (datetime)):  # Checks for both date and datetime
                    formatted_date = col.strftime('%d-%m-%Y %H:%M:%S')
                    processed_row.append(formatted_date)
                elif isinstance(col, Decimal):
                    processed_row.append(float(col))
                else:
                    processed_row.append(col)
            processed_results.append(processed_row)
        for processed_row in processed_results:
            processed_cursor.execute("""
                                INSERT OR REPLACE INTO vccs_cases (
                                    lead_id, time_id, case_number,district_id,district,police_station,police_station_id,
                                    level1_case_nature, level2_case_nature, level3_case_nature,caller_name,phone_number,
                                    final_status_id,final_status_remarks, description, created_at,handed_over_to,is_fir_registered,is_challan_submitted
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, processed_row)
        processed_conn.commit()

    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def vwps_records(db_conn, processed_conn, start_timestamp, end_timestamp):
    try:
        processed_cursor = processed_conn.cursor()

        primary_cursor = db_conn.cursor()

        query = """
        SELECT 
            cfs.lead_id,
            cfs.pucar_time_id,
            cfs.pucar_case_number,
            cfs.pucar_district_id,
            cfs.pucar_district,
            cfs.pucar_police_station,
            cfs.pucar_police_station_id,
            cfs.level1_case_nature,
            cfs.pucar_level2_case_nature,
            cfs.level3_case_nature,
            cfs.pucar_caller_name,
            cfs.pucar_cli,
            cfs.final_status_id,
            cfs.final_status_remarks,
            cfs.pucar_cro_comments,
            cfs.created_at
        FROM 
            `case_final_status` cfs
        WHERE 
            cfs.pucar_status = 'CompCa' AND
            cfs.created_at BETWEEN %s AND %s
        """

        primary_cursor.execute(query, (start_timestamp, end_timestamp))
        rows = primary_cursor.fetchall()

        processed_results = []

        for row in rows:
            processed_row = []
            for col in row:
                if isinstance(col, bytes):
                    try:
                        processed_row.append(col.decode('utf-8'))
                    except UnicodeDecodeError:
                        processed_row.append(int(col))
                elif isinstance(col, (datetime)):  # Checks for both date and datetime
                    formatted_date = col.strftime('%d-%m-%Y %H:%M:%S')
                    processed_row.append(formatted_date)
                elif isinstance(col, Decimal):
                    processed_row.append(float(col))
                else:
                    processed_row.append(col)
            processed_results.append(processed_row)
        for processed_row in processed_results:
            processed_cursor.execute("""
                        INSERT OR REPLACE INTO vwps_cases (
                            lead_id, time_id, case_number,district_id,district,police_station,police_station_id,
                            level1_case_nature, level2_case_nature, level3_case_nature,caller_name,phone_number,
                            final_status_id,final_status_remarks, description, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, processed_row)
        processed_conn.commit()

    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def vcm_records(db_conn, processed_conn, start_timestamp, end_timestamp):
    try:
        processed_cursor = processed_conn.cursor()

        primary_cursor = db_conn.cursor()

        query = """
        SELECT 
            cfs.lead_id,
            cfs.pucar_time_id,
            cfs.pucar_case_number,
            cfs.pucar_district_id,
            cfs.pucar_district,
            cfs.pucar_police_station,
            cfs.level1_case_nature,
            cfs.pucar_level2_case_nature,
            cfs.level3_case_nature,
            cfs.pucar_caller_name,
            cfs.pucar_cli,
            cfs.final_status_id,
            cfs.final_status_remarks,
            cfs.pucar_cro_comments,
            cfs.created_at
        FROM 
            `case_final_status` cfs
        WHERE 
            cfs.pucar_status = 'CompCa' AND
            cfs.created_at BETWEEN %s AND %s
        """

        primary_cursor.execute(query, (start_timestamp, end_timestamp))
        rows = primary_cursor.fetchall()

        processed_results = []

        for row in rows:
            processed_row = []
            for col in row:
                if isinstance(col, bytes):
                    try:
                        processed_row.append(col.decode('utf-8'))
                    except UnicodeDecodeError:
                        processed_row.append(int(col))
                elif isinstance(col, (datetime)):
                    formatted_date = col.strftime('%d-%m-%Y %H:%M:%S')
                    processed_row.append(formatted_date)
                elif isinstance(col, Decimal):
                    processed_row.append(float(col))
                else:
                    processed_row.append(col)
            processed_results.append(processed_row)
        for processed_row in processed_results:
            processed_cursor.execute("""
                        INSERT OR REPLACE INTO vcm_cases (
                            lead_id, time_id, case_number,district_id,district,police_station,
                            level1_case_nature, level2_case_nature, level3_case_nature,caller_name,phone_number,
                            final_status_id,final_status_remarks, description, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, processed_row)
        processed_conn.commit()

    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())

def crime_trends_processing(db_conn):
    try:
        cursor = db_conn.cursor()

        category_query = """
                        SELECT 
                            DATE(FROM_UNIXTIME(time_id)) AS date,
                            district_id,
                            police_station,
                            COUNT(*) AS count,
                            CASE 
                                WHEN level2_case_nature in ('Robbery/Snatching') THEN 'robbery_snatching'
                                WHEN level2_case_nature in ('Dacoity') THEN 'dacoity'
                                WHEN level3_case_nature = 'Motorcycle Theft' THEN 'motorcycle_theft'
                                WHEN level3_case_nature IN ('Cycle Theft','Other Vehicles Theft') THEN 'vehicle_theft'
                                WHEN level2_case_nature in ('Burglary') THEN 'burglary'
                                WHEN level3_case_nature = 'Car Theft' THEN 'car_theft'
                                WHEN level3_case_nature = 'Motorcycle Snatching' THEN 'motorcycle_snatching'
                                WHEN level3_case_nature = 'Car Snatching' THEN 'car_snatching'
                                WHEN level2_case_nature in ('Vehicle Snatching') THEN 'vehicle_snatching'
                                WHEN level2_case_nature = 'Murder' THEN 'murder'
                                WHEN level2_case_nature =  'Kiddnapping / Abduction' THEN 'kidnapping'
                                WHEN level2_case_nature =  'Sexual Assault' THEN 'sexual_assault'
                                WHEN level3_case_nature = 'Aerial Firing' THEN 'firing'
                                ELSE 'other'
                            END AS case_type
                        FROM `15_preprocessed`
                        WHERE status = 'CompCa'
                          AND district_id IS NOT NULL
                          AND police_station IS NOT NULL
                          AND parent_id = 0
                        GROUP BY district_id, police_station, case_type, date
                    """

        # Execute the query
        cursor.execute(category_query)
        query_results = cursor.fetchall()

        results = {}

        for date, district_id, police_station, count, case_type in query_results:
            if isinstance(date, bytes):
                date = date.decode('utf-8')
            if isinstance(district_id, bytes):
                district_id = district_id.decode('utf-8')

            if isinstance(police_station, bytes):
                police_station = police_station.decode('utf-8')

            if isinstance(case_type, bytes):
                case_type = case_type.decode('utf-8')

            if (date, district_id, police_station) not in results:
                results[(date, district_id, police_station)] = {
                    'robbery_snatching': 0,
                    'dacoity': 0,
                    'motorcycle_theft': 0,
                    'burglary': 0,
                    'car_theft': 0,
                    'vehicle_theft': 0,
                    'vehicle_snatching': 0,
                    'car_snatching': 0,
                    'motorcycle_snatching': 0,
                    'murder' : 0,
                    'kidnapping' : 0,
                    'sexual_assault' : 0,
                    'firing' : 0,
                    'other': 0
                }
            results[(date, district_id, police_station)][case_type] += count
        return results

    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def insert_crime_trends(db_connection, results):
    try:
        cursor = db_connection.cursor()
        insert_query = """
        INSERT OR REPLACE INTO crime_trends (
            date, police_station, district_id, burglary, robbery_snatching, 
            dacoity, motorcycle_theft, car_theft, vehicle_theft, vehicle_snatching,
            car_snatching, motorcycle_snatching, murder, kidnapping, sexual_assault, firing
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?)
        """

        # Define default values for each column
        default_values = {
            'robbery_snatching': 0,
            'dacoity': 0,
            'motorcycle_theft': 0,
            'car_theft': 0,
            'vehicle_theft': 0,
            'burglary': 0,
            'vehicle_snatching':0,
            'car_snatching' : 0,
            'motorcycle_snatching' : 0,
            'murder': 0,
            'kidnapping': 0,
            'sexual_assault': 0,
            'firing': 0,
        }

        for (date, district_id, police_station), result in results.items():
            row = {
                'date': date,
                'police_station': f"{police_station}",
                'district_id': district_id
            }
            row.update(default_values)
            row.update(result)

            cursor.execute(insert_query, (
                row['date'], row['police_station'], row['district_id'],
                row['burglary'],row['robbery_snatching'], row['dacoity'],
                row['motorcycle_theft'],row['car_theft'],row['vehicle_theft'],
                row['vehicle_snatching'],row['car_snatching'],row['motorcycle_snatching'],
                row['murder'], row['kidnapping'], row['sexual_assault'] ,row['firing']
            ))

        db_connection.commit()
    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def fir_trends_processing(db_conn):
    try:
        cursor = db_conn.cursor()

        category_query = """
                        SELECT 
                            DATE(dispatch_time) AS date,
                            psca_district_id,
                            psca_ps_name,
                            COUNT(*) AS count,
                            CASE 
                                WHEN level2_case_nature in ('Robbery/Snatching') THEN 'robbery_snatching'
                                WHEN level2_case_nature in ('Dacoity') THEN 'dacoity'
                                WHEN level3_case_nature = 'Motorcycle Theft' THEN 'motorcycle_theft'
                                WHEN level3_case_nature IN ('Cycle Theft','Other Vehicles Theft') THEN 'vehicle_theft'
                                WHEN level2_case_nature in ('Burglary') THEN 'burglary'
                                WHEN level3_case_nature = 'Car Theft' THEN 'car_theft'
                                WHEN level3_case_nature = 'Motorcycle Snatching' THEN 'motorcycle_snatching'
                                WHEN level3_case_nature = 'Car Snatching' THEN 'car_snatching'
                                WHEN level2_case_nature in ('Vehicle Snatching') THEN 'vehicle_snatching'
                                WHEN level2_case_nature = 'Murder' THEN 'murder'
                                WHEN level2_case_nature =  'Kiddnapping / Abduction' THEN 'kidnapping'
                                WHEN level2_case_nature =  'Sexual Assault' THEN 'sexual_assault'
                                WHEN level3_case_nature = 'Aerial Firing' THEN 'firing'
                                ELSE 'other'
                            END AS case_type
                        FROM `leads_in_fir_preprocessed`
                        WHERE psca_district_id IS NOT NULL
                          AND psca_ps_name IS NOT NULL
                        GROUP BY psca_district_id, psca_ps_name, case_type, date
                    """

        # Execute the query
        cursor.execute(category_query)
        query_results = cursor.fetchall()

        results = {}

        for date, district_id, police_station, count, case_type in query_results:
            if isinstance(date, bytes):
                date = date.decode('utf-8')
            if isinstance(district_id, bytes):
                district_id = district_id.decode('utf-8')

            if isinstance(police_station, bytes):
                police_station = police_station.decode('utf-8')

            if isinstance(case_type, bytes):
                case_type = case_type.decode('utf-8')

            if (date, district_id, police_station) not in results:
                results[(date, district_id, police_station)] = {
                    'robbery_snatching': 0,
                    'dacoity': 0,
                    'motorcycle_theft': 0,
                    'burglary': 0,
                    'car_theft': 0,
                    'vehicle_theft': 0,
                    'vehicle_snatching': 0,
                    'car_snatching': 0,
                    'motorcycle_snatching': 0,
                    'murder' : 0,
                    'kidnapping' : 0,
                    'sexual_assault' : 0,
                    'firing' : 0,
                    'other': 0
                }
            results[(date, district_id, police_station)][case_type] += count
        return results

    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def insert_fir_trends(db_connection, results):
    try:
        cursor = db_connection.cursor()
        insert_query = """
        INSERT OR REPLACE INTO fir_trends (
            date, police_station, district_id, burglary, robbery_snatching, 
            dacoity, motorcycle_theft, car_theft, vehicle_theft, vehicle_snatching,
            car_snatching, motorcycle_snatching, murder, kidnapping, sexual_assault, firing
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?,?)
        """

        # Define default values for each column
        default_values = {
            'robbery_snatching': 0,
            'dacoity': 0,
            'motorcycle_theft': 0,
            'car_theft': 0,
            'vehicle_theft': 0,
            'burglary': 0,
            'vehicle_snatching':0,
            'car_snatching' : 0,
            'motorcycle_snatching' : 0,
            'murder': 0,
            'kidnapping': 0,
            'sexual_assault': 0,
            'firing': 0,
        }

        for (date, district_id, police_station), result in results.items():
            row = {
                'date': date,
                'police_station': f"{police_station}",
                'district_id': district_id
            }
            row.update(default_values)
            row.update(result)

            cursor.execute(insert_query, (
                row['date'], row['police_station'], row['district_id'],
                row['burglary'],row['robbery_snatching'], row['dacoity'],
                row['motorcycle_theft'],row['car_theft'],row['vehicle_theft'],
                row['vehicle_snatching'],row['car_snatching'],row['motorcycle_snatching'],
                row['murder'], row['kidnapping'], row['sexual_assault'] ,row['firing']
            ))

        db_connection.commit()
    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def time_wrapper(func, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    print(f"Function {func.__name__} took {end_time - start_time:.4f} seconds to execute.")
    return result


def main(start_date, end_date, start):
    try:
        fir_conv_conn = utils.get_db_connection(configs.FIR_DB)

        processed_conn = utils.get_db_connection(configs.PROCESSED_STATS_MAIN)
        processed_cursor = processed_conn.cursor()

        vccs_conn = db_config.get_vccs_db_connection()
        vwps_conn = db_config.get_vwps_db_connection()
        vcm_conn = db_config.get_vcm_db_connection()

        processed_cursor.execute(f"ATTACH DATABASE '{configs.FIR_DB}' AS fir_db")

        processed_cursor.execute('''
                    CREATE TABLE IF NOT EXISTS processed_data (
                        date TEXT,
                        hour TEXT,
                        district_id INTEGER,
                        police_station TEXT,
                        total_calls INTEGER,
                        generated_cases INTEGER,
                        siraiki INTEGER,
                        punjabi INTEGER,
                        potohari INTEGER,
                        vwps INTEGER,
                        traffic INTEGER,
                        english INTEGER,
                        app_alerts INTEGER,
                        transfered INTEGER,
                        call_backs INTEGER,
                        avg_response_time INTEGER,
                        estimated_response_time INTEGER,
                        video_calls INTEGER,
                        succ_conf_calls INTEGER,
                        unsucc_conf_calls INTEGER,
                        vccs INTEGER,
                        vcm INTEGER,
                        PRIMARY KEY (date, hour, district_id,police_station)
            )
            ''')

        processed_cursor.execute("""
            CREATE TABLE IF NOT EXISTS combined_dashboards (
                date TEXT,
                district_id INTEGER,
                police_station TEXT,
                total_vccs INTEGER,
                under_inquiry_vccs INTEGER,
                escalated_vccs INTEGER,
                fir_vccs INTEGER,
                challan_vccs INTEGER,
                resolved_vccs INTEGER,
                total_vwps INTEGER,
                under_inquiry_vwps INTEGER,
                escalated_vwps INTEGER,
                fir_vwps INTEGER,
                challan_vwps INTEGER,
                resolved_vwps INTEGER,
                total_vcm INTEGER,
                under_inquiry_vcm INTEGER,
                escalated_vcm INTEGER,
                fir_vcm INTEGER,
                challan_vcm INTEGER,
                resolved_vcm INTEGER,
                PRIMARY KEY (date, district_id, police_station)
            )
        """)

        processed_cursor.execute("""
            CREATE TABLE IF NOT EXISTS vccs_cases (
                lead_id INTEGER UNIQUE,
                time_id INTEGER,
                case_number TEXT,
                district_id INTEGER,
                district TEXT,
                police_station TEXT,
                police_station_id INTEGER,
                level1_case_nature TEXT,
                level2_case_nature TEXT,
                level3_case_nature TEXT,
                caller_name TEXT,
                phone_number TEXT,
                final_status_id INTEGER,
                final_status_remarks TEXT,
                description TEXT,
                created_at TEXT,
                handed_over_to INTEGER,
                is_fir_registered INTEGER,
                is_challan_submitted INTEGER
            )
        """)

        processed_cursor.execute("""
            CREATE TABLE IF NOT EXISTS vcm_cases (
                lead_id INTEGER UNIQUE,
                time_id INTEGER,
                case_number TEXT,
                district_id INTEGER,
                district TEXT,
                police_station TEXT,
                level1_case_nature TEXT,
                level2_case_nature TEXT,
                level3_case_nature TEXT,
                caller_name TEXT,
                phone_number TEXT,
                final_status_id INTEGER,
                final_status_remarks TEXT,
                description TEXT,
                created_at TEXT
            )
        """)

        processed_cursor.execute("""
            CREATE TABLE IF NOT EXISTS vwps_cases (
                lead_id INTEGER UNIQUE,
                time_id INTEGER,
                case_number TEXT,
                district_id INTEGER,
                district TEXT,
                police_station TEXT,
                police_station_id INTEGER,
                level1_case_nature TEXT,
                level2_case_nature TEXT,
                level3_case_nature TEXT,
                caller_name TEXT,
                phone_number TEXT,
                final_status_id INTEGER,
                final_status_remarks TEXT,
                description TEXT,
                created_at TEXT
            )
        """)

        processed_conn.commit()

        processed_cursor.execute('''
                    CREATE TABLE IF NOT EXISTS punjab_today (
                        date TEXT,
                        district_id INTEGER,
                        police_station TEXT,
                        murder INTEGER,
                        dacoity INTEGER,
                        aerial_firing INTEGER,
                        rape INTEGER,
                        minorities INTEGER,
                        hurt INTEGER,
                        burglary INTEGER,
                        kidnapping INTEGER,
                        child_abuse INTEGER,
                        robbery_snatching INTEGER,
                        motorcycle_theft INTEGER,
                        dacoity_with_murder INTEGER,
                        car_theft INTEGER,
                        theft INTEGER,
                        terrorist_act INTEGER,
                        other_person INTEGER,
                        other_property INTEGER,
                        PRIMARY KEY (date, district_id,police_station)
            )
            ''')
        processed_conn.commit()

        processed_cursor.execute('''
                    CREATE TABLE IF NOT EXISTS fir_cases (
                        date TEXT,
                        district_id INTEGER,
                        police_station TEXT,
                        murder INTEGER,
                        dacoity INTEGER,
                        aerial_firing INTEGER,
                        rape INTEGER,
                        minorities INTEGER,
                        hurt INTEGER,
                        burglary INTEGER,
                        kidnapping INTEGER,
                        child_abuse INTEGER,
                        robbery_snatching INTEGER,
                        motorcycle_theft INTEGER,
                        dacoity_with_murder INTEGER,
                        car_theft INTEGER,
                        theft INTEGER,
                        terrorist_act INTEGER,
                        other_person INTEGER,
                        other_property INTEGER,
                        PRIMARY KEY (date,district_id,police_station)
            )
            ''')
        processed_conn.commit()

        processed_cursor.execute('''
                           CREATE TABLE IF NOT EXISTS crime_trends (
                               date TEXT,
                               district_id INTEGER,
                               police_station TEXT,
                               dacoity INTEGER,
                               burglary INTEGER,
                               robbery_snatching INTEGER,
                               motorcycle_theft INTEGER,
                               car_theft INTEGER,
                               vehicle_theft INTEGER,
                               vehicle_snatching INTEGER,
                               car_snatching INTEGER,
                               motorcycle_snatching INTEGER,
                               murder INTEGER,
                               sexual_assault INTEGER,
                               firing INTEGER,
                               kidnapping INTEGER,
                               PRIMARY KEY (date,district_id,police_station)
                   )
                   ''')
        processed_conn.commit()

        processed_cursor.execute('''
                   CREATE TABLE IF NOT EXISTS fir_trends (
                       date TEXT,
                       district_id INTEGER,
                       police_station TEXT,
                       dacoity INTEGER,
                       burglary INTEGER,
                       robbery_snatching INTEGER,
                       motorcycle_theft INTEGER,
                       car_theft INTEGER,
                       vehicle_theft INTEGER,
                       vehicle_snatching INTEGER,
                       car_snatching INTEGER,
                       motorcycle_snatching INTEGER,
                       murder INTEGER,
                       sexual_assault INTEGER,
                       firing INTEGER,
                       kidnapping INTEGER,
                       PRIMARY KEY (date,district_id,police_station)
           )
           ''')
        processed_conn.commit()

        current_date = start_date
        while current_date <= end_date:
            if start:
                print(f"{datetime.fromtimestamp(time.time())}: Processing Stats for {current_date}.")
            start_timestamp = utils.date_to_unix_time(current_date.strftime(configs.YMD_TIME))
            end_timestamp = utils.date_to_unix_time(
                (current_date + timedelta(days=configs.DELTA_DAYS)).strftime(configs.YMD_TIME)) - 1

            """Calls Stats Processing & Records Insertion in DB"""
            results = process_date(db_conn, start_timestamp, end_timestamp)
            insert_results(processed_conn, results, current_date.strftime(configs.YM_DATE))

            """Response Time Processing & Records Insertion in DB"""
            response_time(db_conn, processed_conn, start_timestamp, end_timestamp)

            """Processes PUNJAB TODAY AND Inserts in DB"""
            results = process_punjab_today(db_conn, start_timestamp, end_timestamp)
            insert_punjab_today(processed_conn, results, current_date.strftime(configs.YM_DATE))

            current_date_str = current_date.strftime('%Y-%m-%d 00:00:00')
            day_end_str = current_date.strftime('%Y-%m-%d 23:59:59')

            """Porcesses VCCS,VCM & VWPS DASHBOARD STATS AND INSERTING"""
            results = process_combined_dashboard(vccs_conn, vwps_conn, vcm_conn, current_date, day_end_str)
            insert_combined_dashboard_data(processed_conn, results, current_date.strftime(configs.YM_DATE))

            """VCCS Table Migration"""
            vccs_records(vccs_conn, processed_conn, current_date_str, day_end_str)

            """VWPS Table Migration"""
            vwps_records(vwps_conn, processed_conn, current_date_str, day_end_str)

            """VCM Table Migration"""
            vcm_records(vcm_conn, processed_conn, current_date_str, day_end_str)

            """Porcesses FIR CASES AND INSERTING"""
            process_fir_cases(db_conn,processed_conn,start_timestamp, end_timestamp,current_date.strftime(configs.YM_DATE))

            """PROCESSES CRIME TRENDS AND INSERTING"""
            results = crime_trends_processing(db_conn)
            insert_crime_trends(processed_conn,results)

            results = fir_trends_processing(db_conn)
            insert_fir_trends(processed_conn, results)

            current_date += timedelta(days=configs.DELTA_DAYS)

        processed_conn.close()
    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


if __name__ == '__main__':
    start_date = datetime.strptime('17-01-25', '%d-%m-%y')
    end_date = datetime.strptime('17-01-25', '%d-%m-%y')
    main(start_date, end_date, True)