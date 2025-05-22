"""
process_enmities.py

Continuously (every WINDOW_HOURS) finds all response_time cases in the last WINDOW_HOURS
whose level3_case_nature is in a given list, finds any old_enmities within 300 m,
and upserts new pairs into found_enmities_case.
"""

import time
import math
import logging
from psycopg2.extras import execute_values
from psycopg2 import pool as pg_pool
from Utilities.utils import get_processed_db_connection
from Utilities import configs

# Configurable
WINDOW_HOURS = 2
MAX_DISTANCE_METERS = 300

CASE_NATURES = [
    "Murder",
    "Attempt to Murder",
    "Suicide",
    "Attempt to Suicide",
    "Other Assault",
    "Attempt to Kidnap / Abduct",
    "Aerial Firing",
    "Female Kidnapping/ Abduction",
    "Male Kidnapping/ Abduction",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

def haversine(lon1, lat1, lon2, lat2):
    if None in (lon1, lat1, lon2, lat2):
        return None
    lon1, lat1, lon2, lat2 = map(float, (lon1, lat1, lon2, lat2))
    lon1, lat1, lon2, lat2 = map(math.radians, [lon1, lat1, lon2, lat2])
    dlon, dlat = lon2 - lon1, lat2 - lat1
    a = math.sin(dlat/2)**2 + math.cos(lat1)*math.cos(lat2)*math.sin(dlon/2)**2
    return 2 * math.asin(math.sqrt(a)) * 6_371_000  # Earth radius in meters

def main():
    postgresql_pool = pg_pool.SimpleConnectionPool(
        minconn=1, maxconn=3,
        dbname=configs.POSTGRES_PROCESSED_STATS_MAIN['dbname'],
        user=configs.POSTGRES_PROCESSED_STATS_MAIN['user'],
        password=configs.POSTGRES_PROCESSED_STATS_MAIN['password'],
        host=configs.POSTGRES_PROCESSED_STATS_MAIN['host'],
        port=configs.POSTGRES_PROCESSED_STATS_MAIN['port']
    )
    conn = postgresql_pool.getconn()
    try:
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS found_enmities_case (
            date TEXT,
            rt_caller_name TEXT,
            rt_caller_number TEXT,
            rt_district TEXT,
            rt_accepted_time TEXT,
            rt_case_number TEXT NOT NULL,
            enmity_fir_number TEXT NOT NULL,
            rt_police_station TEXT,
            enmities_police_station TEXT NOT NULL,
            rt_lat DOUBLE PRECISION,
            rt_long DOUBLE PRECISION,
            enmity_lat DOUBLE PRECISION,
            enmity_long DOUBLE PRECISION,
            level3_case_nature TEXT,
            distance_meters DOUBLE PRECISION,
            UNIQUE (rt_case_number, enmity_fir_number, rt_police_station, enmities_police_station)
        );
        """)
        conn.commit()
        logging.info("Ensured found_enmities_case table exists.")

        insert_sql = """
        INSERT INTO found_enmities_case (
            date, rt_caller_name, rt_caller_number, rt_district, rt_accepted_time,
            rt_case_number, enmity_fir_number, rt_police_station, enmities_police_station,
            rt_lat, rt_long, enmity_lat, enmity_long, level3_case_nature, distance_meters
        ) VALUES %s
        ON CONFLICT DO NOTHING;
        """

        now_ts = int(time.time())
        window_start = now_ts - WINDOW_HOURS * 3600
        logging.info("Querying response_time from %d to %d", window_start, now_ts)

        placeholders = ', '.join(['%s'] * len(CASE_NATURES))
        params = [window_start, now_ts] + CASE_NATURES
        cursor.execute(f"""
            SELECT
              date, caller_name, caller_number, district_id, accepted_time,
              case_number, long AS rt_long, lat AS rt_lat,
              level3_case_nature, police_station AS rt_police_station
            FROM response_time
            WHERE time_id::bigint BETWEEN %s AND %s
              AND level3_case_nature in ({placeholders})
        """, params)
        rt_rows = cursor.fetchall()
        logging.info("  → %d response_time cases to process", len(rt_rows))

        if not rt_rows:
            logging.info("No new cases found. Exiting.")
            return

        cursor.execute("""
            SELECT fir_number, police_station, longitude, latitude
            FROM old_enmities
        """)
        enm_rows = cursor.fetchall()
        logging.info("  → %d old_enmities rows loaded", len(enm_rows))

        to_insert = []
        for date, caller_name, caller_number, district_id, accepted_time, case_number, rt_long, rt_lat, nature, rt_ps in rt_rows:
            for enm_fir, enm_ps, enm_long, enm_lat in enm_rows:
                dist = haversine(rt_long, rt_lat, enm_long, enm_lat)
                if dist is not None and dist <= MAX_DISTANCE_METERS:
                    to_insert.append((
                        date, caller_name, caller_number,
                        configs.DISTRICTS_DICTIONARY[int(district_id)],
                        accepted_time, case_number, enm_fir,
                        rt_ps, enm_ps, rt_lat, rt_long, enm_lat, enm_long,
                        nature, dist
                    ))

        logging.info("  → %d matching enmity pairs found", len(to_insert))

        if to_insert:
            execute_values(cursor, insert_sql, to_insert, page_size=100)
            conn.commit()
            logging.info("Inserted new pairs into found_enmities_case.")

    except Exception:
        logging.exception("Error during cron job execution")
    finally:
        if conn:
            try:
                postgresql_pool.putconn(conn)
                logging.info("Returned connection to pool.")
            except Exception as pool_err:
                logging.error(f"Error returning connection to pool: {pool_err}")


if __name__ == "__main__":
    main()
