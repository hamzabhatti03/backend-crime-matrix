import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import time
import traceback
from decimal import Decimal
from Utilities import configs, utils, db_config
from ProcessingAgents import police_vehicle_locations_pgs as ps_vec_locs
from ProcessingAgents import scrape_feedbacks_pgs as fb_data
from ProcessingAgents import fir_scraper_pgs as fir_data

'''
Code processing file to incorporate PUNJAB TODAY PROCESSING LOGIC USING CENTRALIZED POSTGRESQL DB
Latest file before this file for processing is pgs_processing_script_v2.py, which includes the optimization changes i.e.,
1) Removed extra tables not used further
2) Caller feedback in response time table to resolve the dependency from main db
3) User activity logs mechanism updated
'''

def process_date(db_connection, log_db_cursor, start_timestamp, end_timestamp):
    try:
        cursor = db_connection.cursor()

        queries = {
            'total_calls': """
                SELECT 
                    district_id, 
                    police_station,
                    HOUR(FROM_UNIXTIME(time_id)) AS hour, 
                    COUNT(*) AS count
                FROM 15_preprocessed
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
                FROM 15_preprocessed
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
                    COUNT(CASE WHEN l.field3 = 'Successful Conference call' THEN 1 END) AS successful_calls,
                    COUNT(CASE WHEN l.field3 = 'FO did not attend the call' THEN 1 END) AS unsuccessful_calls
                FROM 
                    15_preprocessed l
                WHERE 
                    l.district_id IS NOT NULL 
                    AND l.time_id BETWEEN %s AND %s
                    AND l.field3 IS NOT NULL
                GROUP BY 
                    l.district_id, l.police_station, hour;
            """
        }

        results = {}

        for key, query in queries.items():
            cursor.execute(query, (start_timestamp, end_timestamp))

            for row in cursor.fetchall():
                district_id, police_station, hour = row[:3]

                # Decode byte values if applicable
                if isinstance(district_id, bytes):
                    district_id = district_id.decode('utf-8')
                if isinstance(police_station, bytes):
                    police_station = police_station.decode('utf-8')

                if (district_id, police_station, hour) not in results:
                    results[(district_id, police_station, hour)] = {}

                if key == 'total_calls':
                    results[(district_id, police_station, hour)]['total_calls'] = row[3]
                elif key == 'counts':
                    counts_keys = [
                        'siraiki', 'punjabi', 'potohari', 'vwps', 'traffic', 'english',
                        'transfered', 'call_backs', 'vccs', 'vcm', 'generated_cases'
                    ]
                    results[(district_id, police_station, hour)].update({
                        counts_keys[i]: int(row[i + 3]) if isinstance(row[i + 3], Decimal) else row[i + 3]
                        for i in range(len(counts_keys))
                    })
                elif key == 'conference_calls':
                    results[(district_id, police_station, hour)].update({
                        'succ_conf_calls': row[3],
                        'unsucc_conf_calls': row[4]
                    })

        return results
    except Exception as e:
        utils.log_to_database(db_connection, log_db_cursor, "ERROR", traceback.format_exc())


def insert_results(db_connection, log_db_cursor, results, date):
    try:
        cursor = db_connection.cursor()
        insert_query = sql.SQL("""
            INSERT INTO processed_data (
                date, hour, district_id, police_station, total_calls, generated_cases, siraiki, punjabi, potohari, vwps, 
                traffic, english, app_alerts, transfered, call_backs, avg_response_time, estimated_response_time, 
                video_calls, succ_conf_calls, unsucc_conf_calls, vccs, vcm
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (date, hour, district_id, police_station) 
            DO UPDATE SET
                total_calls = EXCLUDED.total_calls,
                generated_cases = EXCLUDED.generated_cases,
                siraiki = EXCLUDED.siraiki,
                punjabi = EXCLUDED.punjabi,
                potohari = EXCLUDED.potohari,
                vwps = EXCLUDED.vwps,
                traffic = EXCLUDED.traffic,
                english = EXCLUDED.english,
                app_alerts = EXCLUDED.app_alerts,
                transfered = EXCLUDED.transfered,
                call_backs = EXCLUDED.call_backs,
                avg_response_time = EXCLUDED.avg_response_time,
                estimated_response_time = EXCLUDED.estimated_response_time,
                video_calls = EXCLUDED.video_calls,
                succ_conf_calls = EXCLUDED.succ_conf_calls,
                unsucc_conf_calls = EXCLUDED.unsucc_conf_calls,
                vccs = EXCLUDED.vccs,
                vcm = EXCLUDED.vcm;
        """)

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
            'avg_response_time': 37,
            'estimated_response_time': 30,
            'video_calls': 0,
            'succ_conf_calls': 0,
            'unsucc_conf_calls': 0,
            'vccs': 0,
            'vcm': 0
        }

        for (district_id, police_station, hour), result in results.items():
            row = {
                'date': date,
                'hour': str(hour),
                'district_id': district_id,
                'police_station': police_station
            }
            row.update(default_values)
            row.update(result)

            try:
                cursor.execute(insert_query, (
                    row['date'], row['hour'], row['district_id'],
                    row['police_station'] if row['police_station'] not in [None, 'null', 'NULL'] else 'UNKNOWN',
                    row['total_calls'], row['generated_cases'], row['siraiki'], row['punjabi'], row['potohari'],
                    row['vwps'], row['traffic'], row['english'], row['app_alerts'], row['transfered'],
                    row['call_backs'],
                    row['avg_response_time'], row['estimated_response_time'], row['video_calls'],
                    row['succ_conf_calls'],
                    row['unsucc_conf_calls'], row['vccs'], row['vcm']
                ))
                # print(f"Inserted/updated: {row}")
            except Exception as e:
                print(f"Error for district {district_id}, station {police_station}, hour {hour}: {e}")
                # db_connection.rollback()  # Roll back the entire transaction on error

        db_connection.commit()
    except Exception as e:
        utils.log_to_database(db_connection, log_db_cursor, "ERROR", traceback.format_exc())


def response_time(primary_conn, log_db_cursor, processed_conn, start_timestamp, end_timestamp):
    try:
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
            MAX(d.start_time) AS start_time,
            l.police_station_id,
            l.police_station,
            l.police_circle,
            l.queue,
            l.district_id AS district_id,
            p.region_category,
            MAX(d.created_time) AS created_time,
            CASE 
                WHEN d.created_time IS NULL THEN 'Manual'
                WHEN d.created_time < FROM_UNIXTIME(l.accepted_time) THEN 'Agent'
                ELSE 'District'
            END AS tab,
            MAX(d.completed_time) AS completed_time,
            l.caller_name,
            l.cli,
            MAX(d.job_status) AS job_status,
            l.caller_location,
            MAX(d.complete_by) AS complete_by,
            l.level1_case_nature,
            l.level2_case_nature,
            l.level3_case_nature,
            MAX(d.reached_lat),
            MAX(d.reached_long),
            l.field3,
            l.lat,
            l.long,
            d.reached_lat,
            d.reached_long,
            l.caller_feedback
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
            l.accepted_time, l.level1_case_nature, l.level2_case_nature, l.level3_case_nature, l.field3,l.caller_feedback;
        """

        primary_cursor.execute(query, (start_timestamp, end_timestamp))
        rows = primary_cursor.fetchall()

        processed_cursor = processed_conn.cursor()
        insert_query = sql.SQL("""
        INSERT INTO response_time (
            date, lead_id, parent_id, time_id, case_number, description, response_time,
            responder_id, accept_time, accepted_time, reached_time, first_arrival_time,
            dispatched_time, start_time, police_station_id, police_station, police_circle,
            queue, district_id, region_category, created_time, tab, completed_time, caller_name, caller_number,
            job_status, caller_location, complete_by, level1_case_nature, level2_case_nature,
            level3_case_nature, responder_lat, responder_long, field3, lat, long, reached_lat, reached_long,
            caller_feedback
        ) VALUES (
            {placeholders}
        )
        ON CONFLICT (lead_id) DO UPDATE SET
            response_time = EXCLUDED.response_time,
            responder_id = EXCLUDED.responder_id,
            accept_time = EXCLUDED.accept_time,
            accepted_time = EXCLUDED.accepted_time,
            reached_time = EXCLUDED.reached_time,
            dispatched_time = EXCLUDED.dispatched_time,
            start_time = EXCLUDED.start_time,
            completed_time = EXCLUDED.completed_time;
        """).format(
            placeholders=sql.SQL(",").join(sql.Placeholder() for _ in range(39))
        )

        for row in rows:
            processed_row = []
            for col in row:
                if isinstance(col, bytes):
                    processed_row.append(col.decode('utf-8'))
                elif isinstance(col, datetime):
                    processed_row.append(col.strftime('%Y-%m-%d %H:%M:%S'))
                elif isinstance(col, Decimal):
                    processed_row.append(float(col))
                elif isinstance(col, float):
                    processed_row.append(int(col))  # Ensure float is cast to int
                else:
                    processed_row.append(col)

            if len(processed_row) != 39:
                raise ValueError(f"Expected 39 values, got {len(processed_row)}: {processed_row}")

            try:
                processed_cursor.execute(insert_query, processed_row)
            except Exception as exec_error:
                print(f"Error with row: {processed_row}\n{exec_error}")
                raise

        processed_conn.commit()

    except psycopg2.Error as db_error:
        utils.log_to_database(primary_conn, log_db_cursor, "DB_ERROR", str(db_error))
        raise
    except Exception as e:
        utils.log_to_database(primary_conn, log_db_cursor, "ERROR", traceback.format_exc())
        raise


def process_fir_cases(db_conn, processed_conn, start_timestamp, end_timestamp, date):
    try:
        cursor = db_conn.cursor()
        processed_cursor = processed_conn.cursor()

        # Categorize and fetch FIR counts
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
                lead_id, district_id, police_station
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

        cursor.execute(query, (start_timestamp, end_timestamp))
        results = cursor.fetchall()

        # FIR data template
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

        # Optimized bulk insert/update query
        insert_query = sql.SQL("""
            INSERT INTO fir_cases (
                date, district_id, police_station, murder, dacoity, aerial_firing, rape, minorities, hurt, burglary,
                kidnapping, child_abuse, robbery_snatching, motorcycle_theft, dacoity_with_murder, car_theft, theft,
                terrorist_act, other_person, other_property
            ) VALUES (
                {placeholders}
            )
            ON CONFLICT (date, district_id, police_station) DO UPDATE SET
                murder = EXCLUDED.murder,
                dacoity = EXCLUDED.dacoity,
                aerial_firing = EXCLUDED.aerial_firing,
                rape = EXCLUDED.rape,
                minorities = EXCLUDED.minorities,
                hurt = EXCLUDED.hurt,
                burglary = EXCLUDED.burglary,
                kidnapping = EXCLUDED.kidnapping,
                child_abuse = EXCLUDED.child_abuse,
                robbery_snatching = EXCLUDED.robbery_snatching,
                motorcycle_theft = EXCLUDED.motorcycle_theft,
                dacoity_with_murder = EXCLUDED.dacoity_with_murder,
                car_theft = EXCLUDED.car_theft,
                theft = EXCLUDED.theft,
                terrorist_act = EXCLUDED.terrorist_act,
                other_person = EXCLUDED.other_person,
                other_property = EXCLUDED.other_property;
        """).format(placeholders=sql.SQL(", ").join(sql.Placeholder() for _ in range(20)))

        # Perform batch insertion
        data_to_insert = [
            (
                row['date'], row['district_id'], row['police_station'], row['murder'], row['dacoity'],
                row['aerial_firing'], row['rape'], row['minorities'], row['hurt'], row['burglary'], row['kidnapping'],
                row['child_abuse'], row['robbery_snatching'], row['motorcycle_theft'], row['dacoity_with_murder'],
                row['car_theft'], row['theft'], row['terrorist_act'], row['other_person'], row['other_property']
            )
            for row in fir_rows.values()
        ]
        processed_cursor.executemany(insert_query.as_string(processed_conn), data_to_insert)
        processed_conn.commit()

    except Exception as e:
        utils.log_to_database(processed_conn, None, "ERROR", traceback.format_exc())


def fir_trends_processing(db_conn, log_db_cursor):
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
                    'murder': 0,
                    'kidnapping': 0,
                    'sexual_assault': 0,
                    'firing': 0,
                    'other': 0
                }
            results[(date, district_id, police_station)][case_type] += count
        return results

    except Exception as e:
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


def insert_fir_trends(db_connection, log_db_cursor, results):
    try:
        cursor = db_connection.cursor()
        insert_query = sql.SQL("""
        INSERT INTO fir_trends (
            date, police_station, district_id, burglary, robbery_snatching, 
            dacoity, motorcycle_theft, car_theft, vehicle_theft, vehicle_snatching,
            car_snatching, motorcycle_snatching , murder, kidnapping, sexual_assault, firing
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (date, district_id, police_station) DO UPDATE SET
            burglary = EXCLUDED.burglary,
            robbery_snatching = EXCLUDED.robbery_snatching,
            dacoity = EXCLUDED.dacoity,
            motorcycle_theft = EXCLUDED.motorcycle_theft,
            car_theft = EXCLUDED.car_theft,
            vehicle_theft = EXCLUDED.vehicle_theft,
            vehicle_snatching = EXCLUDED.vehicle_snatching,
            car_snatching = EXCLUDED.car_snatching,
            motorcycle_snatching = EXCLUDED.motorcycle_snatching,
            murder = EXCLUDED.murder,
            kidnapping = EXCLUDED.kidnapping,
            sexual_assault = EXCLUDED.sexual_assault,
            firing = EXCLUDED.firing
        """)

        # Define default values for each column
        default_values = {
            'robbery_snatching': 0,
            'dacoity': 0,
            'motorcycle_theft': 0,
            'car_theft': 0,
            'vehicle_theft': 0,
            'burglary': 0,
            'vehicle_snatching': 0,
            'car_snatching': 0,
            'motorcycle_snatching': 0,
            'murder': 0,
            'kidnapping': 0,
            'sexual_assault': 0,
            'firing': 0,
        }

        for (date, district_id, police_station), result in results.items():
            if date is None:
                continue
            row = {
                'date': date,
                'police_station': f"{police_station}",
                'district_id': district_id
            }
            row.update(default_values)
            row.update(result)

            cursor.execute(insert_query, (
                row['date'], row['police_station'], row['district_id'],
                row['burglary'], row['robbery_snatching'], row['dacoity'],
                row['motorcycle_theft'], row['car_theft'], row['vehicle_theft'],
                row['vehicle_snatching'], row['car_snatching'], row['motorcycle_snatching'],
                row['murder'], row['kidnapping'], row['sexual_assault'], row['firing']
            ))

        db_connection.commit()
    except Exception as e:
        utils.log_to_database(db_connection, log_db_cursor, "ERROR", traceback.format_exc())
        db_connection.rollback()
    finally:
        cursor.close()


def main(start_date, end_date, start):
    db_conn = db_config.get_db_connection()
    log_db_cursor = db_conn.cursor()

    processed_conn = utils.get_processed_db_connection()  # Function to get PostgreSQL connection

    try:
        with processed_conn.cursor() as processed_cursor:
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
                    PRIMARY KEY (date, hour, district_id, police_station)
                )
            ''')

            processed_cursor.execute('''
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
                                police_circle TEXT,
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
                                reached_lat TEXT,
                                reached_long TEXT,
                                caller_feedback TEXT
                            )
                        ''')

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
                    PRIMARY KEY (date, district_id, police_station)
                )
            ''')

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
                                PRIMARY KEY (date, district_id, police_station)
                            )
                        ''')

            processed_cursor.execute("""
                        CREATE TABLE IF NOT EXISTS remarks (
                            id SERIAL PRIMARY KEY,
                            case_id VARCHAR(255) NOT NULL,
                            assigned_to VARCHAR(255),
                            assigned_by VARCHAR(255),
                            cc VARCHAR(255),
                            remarks TEXT,
                            time_stamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP);
            """)

            processed_cursor.execute("""
                        CREATE TABLE IF NOT EXISTS emergency_i_user_logs (
                            id SERIAL PRIMARY KEY,
                            username VARCHAR(255) UNIQUE NOT NULL,
                            lastseen TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP);
            """)

            """ Add indexes for optimization """
            processed_cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_date_district_ps ON processed_data (date, district_id, police_station)')
            processed_cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_date_district_ps_fir ON fir_cases (date, district_id, police_station)')
            processed_cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_date_district_ps_fir_trend ON fir_trends (date, district_id, police_station)')
            processed_cursor.execute('CREATE INDEX IF NOT EXISTS idx_response_lead_id ON response_time (lead_id)')
            processed_cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_response_date_district ON response_time (date, district_id)')
            processed_cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_response_station ON response_time (police_station_id)')

            processed_conn.commit()

        current_date = start_date
        while current_date <= end_date:
            if start:
                print(f"{datetime.fromtimestamp(time.time())}: Processing Stats for {current_date}.")
            start_timestamp = utils.date_to_unix_time(current_date.strftime(configs.YMD_TIME))
            end_timestamp = utils.date_to_unix_time(
                (current_date + timedelta(days=configs.DELTA_DAYS)).strftime(configs.YMD_TIME)) - 1

            """Calls Stats Processing & Records Insertion in DB"""
            results = process_date(db_conn, log_db_cursor, start_timestamp, end_timestamp)
            insert_results(processed_conn, log_db_cursor, results, current_date.strftime(configs.YM_DATE))

            """Response Time Processing & Records Insertion in DB"""
            response_time(db_conn, log_db_cursor, processed_conn, start_timestamp, end_timestamp)

            """Porcesses FIR CASES AND INSERTING"""
            process_fir_cases(db_conn, processed_conn, start_timestamp, end_timestamp,
                              current_date.strftime(configs.YM_DATE))

            """PROCESSES FIR TRENDS AND INSERTING"""
            results = fir_trends_processing(db_conn, log_db_cursor)
            insert_fir_trends(processed_conn, log_db_cursor, results)

            fir_data.main(current_date)
            fb_data.main()
            ps_vec_locs.main()

            current_date += timedelta(days=configs.DELTA_DAYS)
        if processed_conn:
            if processed_cursor:
                processed_cursor.close()
            processed_conn.close()
        if db_conn:
            if log_db_cursor:
                log_db_cursor.close()
            db_conn.close()
    except Exception as e:
        if processed_conn:
            if processed_cursor:
                processed_cursor.close()
            processed_conn.close()
        if db_conn:
            if log_db_cursor:
                log_db_cursor.close()
            db_conn.close()
        utils.log_to_database(db_conn, log_db_cursor, "ERROR", traceback.format_exc())


if __name__ == '__main__':
    start_date = datetime.strptime('28-02-25', '%d-%m-%y')
    end_date = datetime.strptime('03-03-25', '%d-%m-%y')
    main(start_date, end_date, True)
