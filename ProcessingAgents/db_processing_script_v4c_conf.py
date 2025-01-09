'''
***LATEST BEFORE db_processing_script_v4d***
Code processing file to incorporate PUNJAB TODAY PROCESSING LOGIC USING CENTRALIZED DB. IT INCORPORATES
UPDATED Conference Calls LOGIC AND ITS REPORT. Latest file before this file for processing is db_processing_script_v4b.py
'''
import time
from datetime import datetime, timedelta
from Utilities import configs
from Utilities import utils
from Utilities import db_config
import traceback
import schedule
from decimal import Decimal

db_conn = db_config.get_db_connection()
log_db_cursor = db_conn.cursor()


def process_date(db_connection, start_timestamp, end_timestamp):
    try:
        cursor = db_connection.cursor()

        # AND selected_option NOT IN (selected_option IN ('lahore-15','Sargodha-15','Hafizabad-15','Sahiwal-15','Okara-15','kasur-15','Gujranwala',
        #                    'Sialkot-15','Faisalabad-15','Rawalpindi-15','Khanewal-15','Vehari-15','multan-15','Muzaffargarh-15','Attock-15','sheikhupura-15',
        #                    'M-B-Din-15','Jhelum-15','Pakpattan-15','Gujrat-15','Layyah-15','Chiniot-15','T-T-Singh-15','nankana-sb-15','Bahawalnagar-15',
        #                    'Bahawalpur-15','Rahimyar-Khan-15','Mianwali-15','Narowal-15','Jhang-15','Chakwal-15','Lodhran-15','Khushab-15','Rajanpur-15',
        #                    'D-G-Khan-15','Bhakkar-15','potohari-15','english-15','punjabi-15','siraiki-15','minorities-15'))

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
            start_time TEXT,
            police_station_id INTEGER,
            police_station TEXT,
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
            field3 TEXT
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
            l.police_station_id,
            l.police_station,
            l.queue,
            l.district_id AS district_id,
            MAX(d.job_status) AS job_status,
            MAX(d.caller_location) AS caller_location,
            MAX(d.created_time) AS created_time,
            CASE 
                WHEN d.created_time IS NULL THEN 'Manual'
                WHEN d.created_time < FROM_UNIXTIME(l.accepted_time) THEN 'Agent'
                ELSE 'District'
            END AS tab,
            MAX(d.completed_time) AS completed_time,
            MAX(d.start_time) AS start_time,
            MAX(d.complete_by) AS complete_by,
            MAX(d.name) AS name,
            l.cli,
            p.region_category,
            l.level1_case_nature,
            l.level2_case_nature,
            l.level3_case_nature,
            l.field3
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
                        INSERT OR REPLACE INTO response_time (
                            date, lead_id, parent_id, time_id, case_number, description, response_time,
                            responder_id, accept_time, accepted_time, reached_time, first_arrival_time,
                            police_station_id, police_station, queue, district_id, job_status, caller_location,
                            created_time, tab, completed_time, start_time, complete_by, caller_name, caller_number,
                            region_category, level1_case_nature, level2_case_nature, level3_case_nature,field3
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?)
                    """, processed_row)
        processed_conn.commit()

    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def process_agent_stats(db_connection, start_timestamp, end_timestamp):
    try:
        cursor = db_connection.cursor()
        query = """
                SELECT 
                    l.district_id,
                    l.agent,
                    l.agent_name,
                    HOUR(FROM_UNIXTIME(l.time_id)) AS hour,
                    COUNT(CASE WHEN l.status IN ('hoax', 'Hoax') THEN 1 END) AS hoax_calls,
                    COUNT(CASE WHEN l.status = 'consult' THEN 1 END) AS consult_calls,
                    COUNT(CASE WHEN l.status = 'repeated' THEN 1 END) AS repeated_calls,
                    COUNT(CASE WHEN l.status = 'CompCa' AND parent_id = 0 THEN 1 END) AS generated_cases,
                    COUNT(CASE WHEN cf.feedback_agent_option = 1 AND l.status = 'CompCa' THEN 1 END) AS positive_feedback,
                    COUNT(CASE WHEN cf.feedback_agent_option = 2 AND l.status = 'CompCa' THEN 1 END) AS negative_feedback,
                    AVG(CASE WHEN wrapup_time IS NOT NULL THEN wrapup_time END) AS avg_wrapup_time
                FROM `15_preprocessed` l
                LEFT JOIN `call_feedback_agent_preprocessed` cf ON l.lead_id = cf.lead_id
                WHERE 
                    l.time_id BETWEEN %s AND %s
                    AND l.district_id IS NOT NULL
                    AND l.agent IS NOT NULL
                GROUP BY 
                    district_id, agent, hour;
            """

        results = {}
        cursor.execute(query, (start_timestamp, end_timestamp))
        for district_id, agent, agent_name, hour, hoax_calls, consult_calls, repeated_calls, generated_cases, positive_feedback, negative_feedback, avg_wrapup_time in cursor.fetchall():

            if isinstance(district_id, bytes):
                district_id = district_id.decode('utf-8')
            if isinstance(agent, bytes):
                agent = agent.decode('utf-8')
            if isinstance(agent_name, bytes):
                agent_name = agent_name.decode('utf-8')

            if agent not in results:
                results[agent] = {'agent_name': agent_name, 'district_id': district_id}

            if 'hourly_stats' not in results[agent]:
                results[agent]['hourly_stats'] = {}

            if hour not in results[agent]['hourly_stats']:
                results[agent]['hourly_stats'][hour] = {}

            results[agent]['hourly_stats'][hour]['hoax_calls'] = hoax_calls
            results[agent]['hourly_stats'][hour]['consult_calls'] = consult_calls
            results[agent]['hourly_stats'][hour]['repeated_calls'] = repeated_calls
            results[agent]['hourly_stats'][hour]['generated_cases'] = generated_cases
            results[agent]['hourly_stats'][hour]['positive_feedback'] = positive_feedback
            results[agent]['hourly_stats'][hour]['negative_feedback'] = negative_feedback
            results[agent]['hourly_stats'][hour]['avg_wrapup_time'] = avg_wrapup_time
        return results
    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def insert_agent_stats(db_connection, results, date):
    try:
        cursor = db_connection.cursor()
        insert_query = """
            INSERT OR REPLACE INTO agent_stats (
                date, district_id, hour, agent, agent_name, hoax_calls, consult_calls, repeated_calls, generated_cases, 
                positive_feedback, negative_feedback, avg_wrapup_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

        default_values = {
            'hoax_calls': 0,
            'consult_calls': 0,
            'repeated_calls': 0,
            'generated_cases': 0,
            'positive_feedback': 0,
            'negative_feedback': 0,
            'avg_wrapup_time': 0
        }

        for agent, result in results.items():
            agent_name = result['agent_name']
            district_id = result['district_id']
            for hour, stats in result.get('hourly_stats', {}).items():
                row = {
                    'date': date,
                    'district_id': district_id,
                    'hour': f"{hour}",
                    'agent': agent,
                    'agent_name': agent_name
                }
                row.update(default_values)
                row.update(stats)

                cursor.execute(insert_query, (
                    row['date'], row['district_id'], row['hour'], row['agent'], row['agent_name'],
                    row['hoax_calls'], row['consult_calls'], row['repeated_calls'], row['generated_cases'],
                    row['positive_feedback'], row['negative_feedback'], row['avg_wrapup_time']
                ))
        db_connection.commit()
    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def process_hourly_stats(primary_conn, processed_cursor, processed_conn, start_date, end_date):
    primary_cursor = primary_conn.cursor()
    try:
        current_date = start_date
        primary_cursor.execute("""
                    SELECT 
                        DATE(FROM_UNIXTIME(time_id)) AS readable_date,
                        DATE_FORMAT(FROM_UNIXTIME(time_id), '%H:%i') AS time,
                        HOUR(FROM_UNIXTIME(time_id)) AS hour,
                        COUNT(CASE WHEN status IN ('hoax', 'Hoax') THEN 1 END) AS hoax_count,
                        COUNT(CASE WHEN status = 'consult' THEN 1 END) AS consult_call_count,
                        COUNT(CASE WHEN status = 'CompCa' THEN 1 END) AS CompCa_count
                    FROM
                        `15_preprocessed`
                    WHERE
                        DATE(FROM_UNIXTIME(time_id)) = %s
                    GROUP BY 
                        readable_date, hour
                    ORDER BY 
                        readable_date, hour
""", (current_date,))

        rows = primary_cursor.fetchall()

        for row in rows:
            processed_cursor.execute("""
                INSERT OR REPLACE INTO hourly_stats (
                    date, time, hour, hoax_call_count, consult_call_count, generated_case_count
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(date, hour)
                DO UPDATE SET 
                    time = excluded.time,
                    hoax_call_count = excluded.hoax_call_count,
                    consult_call_count = excluded.consult_call_count,
                    generated_case_count = excluded.generated_case_count
            """, (row[0], row[1], row[2], row[3], row[4], row[5]))

        processed_conn.commit()

    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def process_henious_crime_stats(db_connection, start_timestamp, end_timestamp):
    try:
        cursor = db_connection.cursor()
        query = """
            SELECT district_id,
                   HOUR(FROM_UNIXTIME(time_id)) AS hour,
                   SUM(CASE WHEN level2_case_nature = 'Dacoity' THEN 1 ELSE 0 END) AS dacoity,
                   SUM(CASE WHEN level2_case_nature = 'Robbery/Snatching' THEN 1 ELSE 0 END) AS robbery,
                   SUM(CASE WHEN level2_case_nature = 'Murder' THEN 1 ELSE 0 END) AS murder,
                   SUM(CASE WHEN level3_case_nature = 'Sexual Assault/ Harrasment To Women' THEN 1 ELSE 0 END) AS women_harrassment,
                   SUM(CASE WHEN level2_case_nature = 'Traffic Accidents' THEN 1 ELSE 0 END) AS traffic_accidents,
                   SUM(CASE WHEN level3_case_nature = 'Rape' THEN 1 ELSE 0 END) AS rape
            FROM `15_preprocessed`
            WHERE district_id IS NOT NULL 
            AND time_id BETWEEN %s AND %s
            GROUP BY district_id, hour
            """

        cursor.execute(query, (start_timestamp, end_timestamp))

        results = {}

        for row in cursor.fetchall():
            district_id, hour, dacoity, robbery, murder, women_harrassment, traffic_accidents, rape = row
            if isinstance(district_id, bytes):
                district_id = district_id.decode('utf-8')
            if isinstance(hour, Decimal):
                hour = int(hour)
            if isinstance(dacoity, Decimal):
                dacoity = int(dacoity)
            if isinstance(robbery, Decimal):
                robbery = int(robbery)
            if isinstance(murder, Decimal):
                murder = int(murder)
            if isinstance(women_harrassment, Decimal):
                women_harrassment = int(women_harrassment)
            if isinstance(traffic_accidents, Decimal):
                traffic_accidents = int(traffic_accidents)
            if isinstance(rape, Decimal):
                rape = int(rape)
            if district_id not in results:
                results[district_id] = {}

            results[district_id][hour] = {
                'dacoity': dacoity,
                'robbery': robbery,
                'murder': murder,
                'women_harrassment': women_harrassment,
                'traffic_accidents': traffic_accidents,
                'rape': rape
            }

        return results
    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def insert_henious_crimes(db_connection, results, date):
    try:
        cursor = db_connection.cursor()
        insert_query = """
            INSERT OR REPLACE INTO henious_crime_calls (
                date, hour, district_id, dacoity, robbery, murder, women_harrassment, traffic_accidents, rape
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

        default_values = {
            'dacoity': 0,
            'robbery': 0,
            'murder': 0,
            'women_harrassment': 0,
            'traffic_accidents': 0,
            'rape': 0
        }

        for district_id, hour_data in results.items():
            for hour, result in hour_data.items():
                row = {
                    'date': date,
                    'hour': hour,
                    'district_id': district_id
                }
                row.update(default_values)
                row.update(result)

                cursor.execute(insert_query, (
                    row['date'], row['hour'], row['district_id'], row['dacoity'], row['robbery'], row['murder'],
                    row['women_harrassment'], row['traffic_accidents'], row['rape']
                ))

        db_connection.commit()

    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def process_alerts(conn, current_date):
    try:
        cursor = conn.cursor()

        alerts_table = """
            CREATE TABLE IF NOT EXISTS Alerts (
                id INTEGER PRIMARY KEY,
                date TEXT NOT NULL,
                shift TEXT NOT NULL,
                district_id INTEGER NULL,
                count INTEGER NOT NULL,
                lead_ids TEXT NOT NULL,
                response_time REAL NOT NULL,    
                UNIQUE(date, shift, district_id,lead_ids)
            )
            """
        cursor.execute(alerts_table)

        # Inserts All Records where response_time is either less than 3 minutes or greater than 1 hour
        insert_query = f"""
            INSERT OR REPLACE INTO Alerts (date,shift,district_id,count,lead_ids,response_time)
            SELECT
                date,
                CASE
                    WHEN strftime('%H', datetime(time_id + 18000, 'unixepoch')) BETWEEN '08' AND '19' THEN '8am-8pm'
                    ELSE '8pm-8am'
                END AS shift,
                district_id,
                COUNT(lead_id) AS case_count,
                GROUP_CONCAT(lead_id) AS lead_ids,
                AVG(response_time) AS avg_response_time
            FROM
                response_time
            WHERE
                response_time < 180
                AND district_id IS NOT NULL
                AND date = '{current_date}'
            GROUP BY
                date,
                shift,
                district_id
            HAVING
                AVG(response_time) < 180;
            """
        # "                (response_time < 180 OR response_time > 3600)" # After WHERE
        # "                AVG(response_time) < 180 OR AVG(response_time) > 3600;" # After HAVING
        cursor.execute(insert_query, )

        conn.commit()
    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def process_negative_feedbacks(primary_conn, processed_conn, processed_cursor, start_timestamp, end_timestamp):
    try:
        primary_cursor = primary_conn.cursor()
        query = """
            SELECT 
                CONCAT('psca-lahore-15-', l.agent, '-', l.cli, '-',
                   DATE_FORMAT(FROM_UNIXTIME(l.time_id), '%d-%m-%Y'), '-', 
                   l.call_id) AS call_id,
                l.agent,
                l.agent_name,
                DATE_FORMAT(FROM_UNIXTIME(l.time_id), '%d-%m-%Y') AS date
            FROM 
                `call_feedback_agent_preprocessed` cf
            JOIN 
                `15_preprocessed` l
            ON 
                l.lead_id = cf.lead_id
            WHERE
                l.district_id IS NOT NULL 
                AND l.time_id BETWEEN %s AND %s
                AND l.status = 'CompCa'
                AND cf.feedback_agent_option = 2
"""
        primary_cursor.execute(query, (start_timestamp, end_timestamp))
        results = primary_cursor.fetchall()

        insert_query = """
            INSERT OR REPLACE INTO negative_feedback_calls (call_id, agent, agent_name,date)
            VALUES (?, ?, ?, ?)
"""
        processed_results = []
        for row in results:
            processed_row = []
            for value in row:
                if isinstance(value, bytes):
                    processed_row.append(value.decode('utf-8'))
                else:
                    processed_row.append(value)
            processed_results.append(tuple(processed_row))

        for row in processed_results:
            processed_cursor.execute(insert_query, row)
            processed_conn.commit()

    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def process_fir_conversion(leads_conn, fir_conv_conn, from_date, to_date):
    # Reuse database connections
    try:
        cursor = fir_conv_conn.cursor()
        leads_cursor = leads_conn.cursor()

        all_window_response = []
        total_generated_cases = 0
        total_fir_generated = 0

        # Initialize sliding window parameters
        current_start_date = from_date

        start_date_unix = utils.date_to_unix_day_start_end(from_date.strftime(configs.YM_DATE), is_start=True)
        end_date_unix = utils.date_to_unix_day_start_end(to_date.strftime(configs.YM_DATE), is_start=False)

        filters = []
        params = []

        filters.append("status = 'CompCa'")
        filters.append("level1_case_nature IN ('Crime Against Person','Crime Against Property')")
        filters.append("time_id BETWEEN ? AND ?")
        params.extend([start_date_unix, end_date_unix])

        where_clause = " AND ".join(filters)

        # Step 1: Get generated cases for this window
        leads_in_query = f"""
                SELECT lead_id, level1_case_nature, level3_case_nature, district_id, police_station_id,cli,caller_name
                FROM 15_preprocessed
                WHERE {where_clause}
            """
        leads_cursor.execute(leads_in_query, params)
        leads = leads_cursor.fetchall()

        if not leads:
            return []

        lead_ids = [str(lead["lead_id"]) for lead in leads if leads]
        leads_by_case_nature = {}

        for lead in leads:
            case_nature = lead["level3_case_nature"]
            if case_nature not in leads_by_case_nature:
                leads_by_case_nature[case_nature] = {
                    "lead_ids": [],
                    "generated_cases": 0,
                    "firs_generated": 0,
                    "cli": "",
                    "caller_name": "",
                    "category": "",
                    "district": "",
                    "police_station": ""
                }
            leads_by_case_nature[case_nature]["lead_ids"].append(lead["lead_id"])
            leads_by_case_nature[case_nature]["generated_cases"] += 1
            leads_by_case_nature[case_nature]["category"] = lead["level1_case_nature"]
            leads_by_case_nature[case_nature]["cli"] = lead["cli"]
            leads_by_case_nature[case_nature]["caller_name"] = lead["caller_name"]
            leads_by_case_nature[case_nature]["district"] = configs.DISTRICTS_DICTIONARY[int(lead["district_id"])]
            leads_by_case_nature[case_nature]["police_station"] = lead["police_station_id"]

        # Step 2: Get FIRs for this window
        if lead_ids:
            firs_query = f"""
                    SELECT
                        lf.lead_id,
                        SUM(CASE WHEN lf.fir_no IS NOT NULL THEN 1 ELSE 0 END) AS FIRs
                    FROM leads_in_fir lf
                    WHERE lf.lead_id IN ({','.join(['?' for _ in lead_ids])})
                    GROUP BY lf.lead_id
                """
            cursor.execute(firs_query, lead_ids)
            firs = cursor.fetchall()

            fir_dict = {row['lead_id']: (row['FIRs']) for row in firs}
        else:
            fir_dict = {}

        # Step 3: Update leads_by_case_nature with FIRs
        if leads_by_case_nature:
            for case_nature, data in leads_by_case_nature.items():
                for lead_id in data["lead_ids"]:
                    firs = fir_dict.get(lead_id, 0)
                    data["firs_generated"] += firs
                    if data["firs_generated"] > data["generated_cases"]:
                        data["firs_generated"] = data["generated_cases"]

            for case_nature, data in leads_by_case_nature.items():
                conversion_rate = (data["firs_generated"] / data["generated_cases"]) * 100 if data[
                                                                                                  "generated_cases"] > 0 else 0
                total_generated_cases += data["generated_cases"]
                total_fir_generated += data["firs_generated"]

                all_window_response.append({
                    "date": from_date.strftime(configs.YM_DATE),
                    "category": data["category"],
                    "police_station": data["police_station"],
                    "caller_name": data["caller_name"],
                    "cli": data["cli"],
                    "district": data["district"],
                    "case_nature": case_nature,
                    "generated_cases": data["generated_cases"],
                    "firs_generated": data["firs_generated"],
                    "conversion_rate": f"{conversion_rate:.2f}%"
                })

        result = utils.matching_category(all_window_response)

        return result
    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def insert_fir_records(processed_conn, result):
    try:
        cursor = processed_conn.cursor()

        if not result:
            return

        insert_query = """
            INSERT OR REPLACE INTO fir_conversion (
                CAW_fir_conversion, CAW_fir, CAW_cases,
                CACH_fir_conversion, CACH_fir, CACH_cases,
                CAP_fir_conversion, CAP_fir, CAP_cases,
                CAPRO_fir_conversion, CAPRO_fir, CAPRO_cases,
                date
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

        data = (
            result.get('CAW_fir_conversion', 0),
            result.get('CAW_fir', 0),
            result.get('CAW_cases', 0),
            result.get('CACH_fir_conversion', 0),
            result.get('CACH_fir', 0),
            result.get('CACH_cases', 0),
            result.get('CAP_fir_conversion', 0),
            result.get('CAP_fir', 0),
            result.get('CAP_cases', 0),
            result.get('CAPRO_fir_conversion', 0),
            result.get('CAPRO_fir', 0),
            result.get('CAPRO_cases', 0),
            result.get('date')
        )

        cursor.execute(insert_query, data)

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
                    WHEN level3_case_nature in ('Other Burglary','Shop Burglary','House Burglary') THEN 'burglary'
                    WHEN level3_case_nature = 'Car Theft' THEN 'car_theft'
                    WHEN level3_case_nature IN ('Mobile Theft','Any Other Theft','Cattle theft',
                            'Transformer/ Motor Theft','Pick Pocketing','Purse / Wallet / Luggage Theft',
                            'Cycle Theft','Weapon Theft','Other Vehicles Theft') THEN 'theft'
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


def process_fir_cases(processed_conn, date):
    try:
        cursor = processed_conn.cursor()

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
            FROM response_time
            WHERE date = ?
        )
        SELECT 
            c.category,
            c.district_id,
            c.police_station,
            COUNT(DISTINCT f.lead_id) as fir_count
        FROM categorized_cases c
        LEFT JOIN leads_in_fir f ON c.lead_id = f.lead_id 
        WHERE c.category IS NOT NULL
            AND c.police_station IS NOT NULL
            AND c.district_id IS NOT NULL
        GROUP BY c.district_id, c.police_station, c.category
        """

        cursor.execute(query, (date,))
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
            cursor.execute(insert_query, (
                row['date'], row['district_id'], row['police_station'], row['murder'], row['dacoity'],
                row['aerial_firing'], row['rape'], row['minorities'], row['hurt'], row['burglary'], row['kidnapping'],
                row['child_abuse'], row['robbery_snatching'], row['motorcycle_theft'], row['dacoity_with_murder'],
                row['car_theft'], row['theft'], row['terrorist_act'], row['other_person'], row['other_property']
            ))

            processed_conn.commit()

    except Exception as e:
        utils.log_to_database(processed_conn, None, "ERROR", traceback.format_exc())


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

        processed_conn.commit()

        processed_cursor.execute('''
                    CREATE TABLE IF NOT EXISTS agent_stats (
                        date TEXT,
                        district_id INTEGER,
                        hour TEXT,
                        agent INTEGER,
                        agent_name TEXT,
                        hoax_calls INTEGER,
                        consult_calls INTEGER,
                        repeated_calls INTEGER,
                        generated_cases INTEGER,
                        positive_feedback TEXT,
                        negative_feedback TEXT,
                        avg_wrapup_time REAL,
                        PRIMARY KEY (date, hour, agent)
            )
            ''')
        processed_conn.commit()

        processed_cursor.execute("""
                CREATE TABLE IF NOT EXISTS hourly_stats (
                    date TEXT,
                    time TEXT,
                    hour TEXT,
                    hoax_call_count INTEGER,
                    consult_call_count INTEGER,
                    generated_case_count INTEGER,
                    PRIMARY KEY (date,hour)
                )
            """)

        processed_cursor.execute("""
                CREATE TABLE IF NOT EXISTS henious_crime_calls (
                    date TEXT,
                    hour TEXT,
                    district_id INTEGER,
                    dacoity INTEGER,
                    robbery INTEGER,
                    murder INTEGER,
                    women_harrassment INTEGER,
                    traffic_accidents INTEGER,
                    rape INTEGER,
                    PRIMARY KEY (date, hour, district_id)
            )
            """)
        processed_conn.commit()

        processed_cursor.execute("""
                CREATE TABLE IF NOT EXISTS negative_feedback_calls (
                    call_id TEXT UNIQUE,
                    agent TEXT,
                    agent_name TEXT,
                    date TEXT
                );
        """)

        processed_conn.commit()

        processed_conn.execute("""
            CREATE TABLE IF NOT EXISTS fir_conversion (
                date TEXT UNIQUE,
                CAW_fir_conversion REAL,
                CAW_fir INTEGER,
                CAW_cases INTEGER,
                CACH_fir_conversion REAL,
                CACH_fir INTEGER,
                CACH_cases INTEGER,
                CAP_fir_conversion REAL,
                CAP_fir INTEGER,
                CAP_cases INTEGER,
                CAPRO_fir_conversion REAL,
                CAPRO_fir INTEGER,
                CAPRO_cases INTEGER
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

            """Agent Stats Processing & Records Insertion in DB"""
            agent_results = process_agent_stats(db_conn, start_timestamp, end_timestamp)
            insert_agent_stats(processed_conn, agent_results, current_date.strftime(configs.YM_DATE))

            """Response Time Processing & Records Insertion in DB"""
            response_time(db_conn, processed_conn, start_timestamp, end_timestamp)

            """Hourly Stats Processing & Records Insertion in DB"""
            process_hourly_stats(db_conn, processed_cursor, processed_conn, current_date.strftime(configs.YM_DATE),
                                 (current_date + timedelta(days=configs.DELTA_DAYS)).strftime(configs.YM_DATE))

            """Heinous Stats Processing & Records Insertion in DB"""
            henious_crimes_results = process_henious_crime_stats(db_conn, start_timestamp, end_timestamp)
            insert_henious_crimes(processed_conn, henious_crimes_results, current_date.strftime(configs.YM_DATE))

            """Alerts Processing & Records Insertion in DB"""
            process_alerts(processed_conn, current_date.strftime(configs.YM_DATE))

            """Processes Negative Feedbacks & Records Insertion in DB"""
            process_negative_feedbacks(db_conn, processed_conn, processed_cursor, start_timestamp, end_timestamp)

            """Processes FIR Conversion & Insertion in DB"""
            fir_results = process_fir_conversion(db_conn, fir_conv_conn, current_date,
                                                 (current_date + timedelta(days=configs.DELTA_DAYS)))
            insert_fir_records(processed_conn, fir_results)

            """Processes PUNJAB TODAY AND Inserts in DB"""
            results = process_punjab_today(db_conn, start_timestamp, end_timestamp)
            insert_punjab_today(processed_conn, results, current_date.strftime(configs.YM_DATE))

            """Porcesses FIR CASES AND INSERTING"""
            process_fir_cases(processed_conn, current_date.strftime(configs.YM_DATE))

            current_date += timedelta(days=configs.DELTA_DAYS)

        processed_conn.close()
    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


if __name__ == '__main__':
    start_date = datetime.strptime('18-12-24', '%d-%m-%y')
    end_date = datetime.strptime('18-12-24', '%d-%m-%y')
    # start_date = datetime.now()  # Current date
    # end_date = datetime.now()
    main(start_date, end_date, True)
