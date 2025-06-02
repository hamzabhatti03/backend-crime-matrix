"""
Data Aggregation and Metrics Processing Script for igp_insights DASHBOARD

This script aggregates operational and performance metrics from multiple databases (test1124 , db_predictive_policing, 1787_db and db_child_safety)
for crime statistics, police response tracking, and service quality monitoring. It calculates comparative
metrics across predefined time periods (weekly, monthly, yearly and 90 days) and stores the results in a centralized
'igp_insights' table for reporting and analysis.

Key Features:
1. **Multi-Database Integration**:
   - Connects to PostgreSQL (source, predictive policing) and MySQL (VCCS, 1787 complaints) databases
   - Uses environment variables for secure database credentials

2. **Time-Period Analysis**:
   - Compares current vs previous periods for trend detection
   - Supported periods: daily week/month comparisons, 15-day, 90-day, and yearly comparisons

3. **Metric Categories**:
   - Conference Call Success Rates
   - Negative Caller Feedback
   - Crime Statistics (Terrorism, Dacoity, Robbery, Rape/Murder)
   - Response Time Alerts (>35 minutes)
   - Crime Reoccurrence Patterns
   - Child Missing/Found Reports
   - Complaint Categories (Non-FIR registration, Police complaints)
   - Service Request Status (Pending/Complete/In Progress)
   - Critical Incident Monitoring (Political, Religious, Foreign-related issues)

4. **Automatic Table Management**:
   - Creates 'igp_insights' table if not exists
   - Uses upsert (INSERT ... ON CONFLICT) for idempotent updates

5. **Error Handling**:
   - Automatic reconnection for database connections
   - Comprehensive logging of operations and errors

6. **Data Flow**:
   - Executes category-specific SQL queries
   - Calculates percentage changes between periods
   - Stores results with timestamps in centralized metrics table

Dependencies:
- Environment variables for database credentials
- Predefined database schema structures
- Required Python modules: logging, datetime, psycopg2, mysql-connector-python

Execution Flow:
1. Establish database connections
2. Create metrics table if needed
3. For each period type:
   a. Calculate date ranges
   b. Execute all metric processors
   c. Collect results with percentage changes
4. Insert/update metrics in database
5. Clean up connections

The script is designed for automated daily execution to maintain updated metrics for police performance
monitoring and crime pattern analysis.
"""

import logging
from datetime import datetime, timedelta
import psycopg2
import os
import sys
import mysql.connector
from mysql.connector import Error as MySQLError
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from Utilities.utils import get_processed_db_connection
from Utilities.db_config import get_1787_db_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Define periods as per your query
PERIODS = {
    "week": {
        "current": {
            "start": lambda today: today - timedelta(days=7),
            "end": lambda today: today
        },
        "previous": {
            "start": lambda today: today - timedelta(days=14),
            "end": lambda today: today - timedelta(days=7)
        },
    },
    "month": {
        "current": {
            "start": lambda today: today - timedelta(days=30),
            "end": lambda today: today
        },
        "previous": {
            "start": lambda today: today - timedelta(days=60),
            "end": lambda today: today - timedelta(days=30)
        }
    },
    "last90days": {
        "current": {
            "start": lambda today: today - timedelta(days=90),
            "end": lambda today: today
        },
        "previous": {
            "start": lambda today: today - timedelta(days=180),
            "end": lambda today: today - timedelta(days=90)
        }
    },
    "last15days_yearly": {
        "current": {
            "start": lambda today: today - timedelta(days=14),
            "end": lambda today: today - timedelta(days=1)
        },
        "previous": {
            "start": lambda today: (today - timedelta(days=14)).replace(year=today.year - 1),
            "end": lambda today: (today - timedelta(days=1)).replace(year=today.year - 1)
        }
    },
}

def connect_to_mysql():
    try:
        vccs_conn = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('VCCS_DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            buffered=True  # Ensure buffered cursor
        )

        vwps_conn = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('VWPS_DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            buffered=True  # Ensure buffered cursor
        )

        vcm_conn = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            database=os.getenv('VCM_DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            buffered=True  # Ensure buffered cursor
        )

        if vccs_conn.is_connected() and vwps_conn.is_connected():
            logging.info("Successfully connected to VCCS,VCM AND VWPS database")
            return vccs_conn, vwps_conn, vcm_conn
        else:
            logging.error("Failed to establish VCCS AND VWPS database connection")
            raise MySQLError("Connection not established")
    except MySQLError as e:
        logging.error(f"Error connecting to MySQL: {str(e)}", exc_info=True)
        raise


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS igp_insights (
                metric_name VARCHAR(50) NOT NULL,
                period_type VARCHAR(20) NOT NULL,
                district_id INTEGER NULL,
                police_station VARCHAR(50) NULL,
                current_start_date DATE NOT NULL,
                current_end_date DATE NOT NULL,
                previous_start_date DATE NOT NULL,
                previous_end_date DATE NOT NULL,
                current_count INTEGER NOT NULL,
                previous_count INTEGER NOT NULL,
                percentage_change NUMERIC(8,2) NOT NULL,
                computed_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY (metric_name, period_type, district_id, police_station)
            );
        """)
    conn.commit()
    logging.info("Ensured igp_insights table exists.")


def calculate_dates(period, today):
    current_start = PERIODS[period]["current"]["start"](today).strftime('%Y-%m-%d')
    current_end = PERIODS[period]["current"]["end"](today).strftime('%Y-%m-%d')
    previous_start = PERIODS[period]["previous"]["start"](today).strftime('%Y-%m-%d')
    previous_end = PERIODS[period]["previous"]["end"](today).strftime('%Y-%m-%d')
    return current_start, current_end, previous_start, previous_end



# Query for conference calls
def get_conference_call_query():
    return """
        SELECT 
            district_id,
            police_station,
            SUM(CASE 
                WHEN DATE(date) >= %(current_start)s AND DATE(date) < %(current_end)s 
                     AND field3 = 'Successful Conference call' THEN 1 ELSE 0 
                END) AS current_success_count,
            SUM(CASE 
                WHEN DATE(date) >= %(previous_start)s AND DATE(date) < %(previous_end)s 
                     AND field3 = 'Successful Conference call' THEN 1 ELSE 0 
                END) AS previous_success_count,
            SUM(CASE 
                WHEN DATE(date) >= %(current_start)s AND DATE(date) < %(current_end)s 
                     AND field3 IN ('Successful Conference call', 'FO did not attend the call', 
                                    'Number Powered Off', 'Out of PS Jurisdiction') THEN 1 ELSE 0 
                END) AS current_total_count,
            SUM(CASE 
                WHEN DATE(date) >= %(previous_start)s AND DATE(date) < %(previous_end)s 
                     AND field3 IN ('Successful Conference call', 'FO did not attend the call', 
                                    'Number Powered Off', 'Out of PS Jurisdiction') THEN 1 ELSE 0 
                END) AS previous_total_count
        FROM response_time
        WHERE parent_id = 0
          AND district_id IS NOT NULL
          AND police_station IS NOT NULL
          AND district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
          AND DATE(date) >= %(previous_start)s
          AND DATE(date) < %(current_end)s
        GROUP BY district_id, police_station;
    """

# Query for negative caller feedback
def get_negative_feedback_query():
    return """
        SELECT 
            district_id,
            police_station,
            SUM(CASE 
                WHEN DATE(date) >= %(current_start)s 
                     AND DATE(date) < %(current_end)s 
                     AND caller_feedback = 'Negative' 
                THEN 1 ELSE 0 
            END) AS current_negative_count,
            SUM(CASE 
                WHEN DATE(date) >= %(previous_start)s 
                     AND DATE(date) < %(previous_end)s 
                     AND caller_feedback = 'Negative' 
                THEN 1 ELSE 0 
            END) AS previous_negative_count
        FROM response_time
        WHERE caller_feedback IS NOT NULL
          AND district_id IS NOT NULL
          AND police_station IS NOT NULL
          AND district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
          AND DATE(date) >= %(previous_start)s
          AND DATE(date) < %(current_end)s
        GROUP BY district_id, police_station;
    """

def get_dashboard_categories_query():
    return """
        SELECT 
            district_id,
            police_station,
            SUM(CASE 
                WHEN level3_case_nature IN ('Firing on Police', 'Suicidal Attack/ Bomb Blast/ Terrorist Attack')
                     AND DATE(date) >= %(current_start)s 
                     AND DATE(date) < %(current_end)s 
                THEN 1 ELSE 0 
            END) AS terrorism_current,
            SUM(CASE 
                WHEN level3_case_nature IN (
                    'Highway/Road/Street Dacoity', 'House Dacoity', 'Any Other Dacoity', 
                    'Shop Dacoity', 'Cattle Dacoity', 'Patrol Pump Dacoity', 'Jewellery Shop Dacoity'
                )
                AND DATE(date) >= %(current_start)s 
                AND DATE(date) < %(current_end)s 
            THEN 1 ELSE 0 
            END) AS dacoity_current,
            SUM(CASE 
                WHEN level3_case_nature IN (
                    'Highway/Road/Street Robbery', 'Any Other Robbery', 'Cattle Robbery', 'House Robbery',
                    'Shop Robbery', 'Patrol Pump Robbery', 'Bank/Money Exchange/ ATM Robbery', 'Car Snatching',
                    'Other Vehicles Snatching', 'Snatching/Jhapatta', 'Motorcycle Snatching', 'Jewellery Shop Robbery',
                    'Robbery with Murder'
                )
                AND DATE(date) >= %(current_start)s 
                AND DATE(date) < %(current_end)s 
            THEN 1 ELSE 0 
            END) AS robbery_snatching_current,
            SUM(CASE 
                WHEN level3_case_nature IN ('Rape', 'Child Abuse / Molestation')
                AND DATE(date) >= %(current_start)s 
                AND DATE(date) < %(current_end)s 
            THEN 1 ELSE 0 
            END) AS rape_sodomy_current,
            SUM(CASE 
                WHEN level3_case_nature = 'Murder'
                AND DATE(date) >= %(current_start)s 
                AND DATE(date) < %(current_end)s 
            THEN 1 ELSE 0 
            END) AS murder_current,
            SUM(CASE 
                WHEN level3_case_nature = 'Dacoity with Murder'
                AND DATE(date) >= %(current_start)s 
                AND DATE(date) < %(current_end)s 
            THEN 1 ELSE 0 
            END) AS dacoity_with_murder_current,
            SUM(CASE 
                WHEN level3_case_nature IN ('Firing on Police', 'Suicidal Attack/ Bomb Blast/ Terrorist Attack')
                     AND DATE(date) >= %(previous_start)s 
                     AND DATE(date) < %(previous_end)s 
            THEN 1 ELSE 0 
            END) AS terrorism_previous,
            SUM(CASE 
                WHEN level3_case_nature IN (
                    'Highway/Road/Street Dacoity', 'House Dacoity', 'Any Other Dacoity', 
                    'Shop Dacoity', 'Cattle Dacoity', 'Patrol Pump Dacoity', 'Jewellery Shop Dacoity'
                )
                AND DATE(date) >= %(previous_start)s 
                AND DATE(date) < %(previous_end)s 
            THEN 1 ELSE 0 
            END) AS dacoity_previous,
            SUM(CASE 
                WHEN level3_case_nature IN (
                    'Highway/Road/Street Robbery', 'Any Other Robbery', 'Cattle Robbery', 'House Robbery',
                    'Shop Robbery', 'Patrol Pump Robbery', 'Bank/Money Exchange/ ATM Robbery', 'Car Snatching',
                    'Other Vehicles Snatching', 'Snatching/Jhapatta', 'Motorcycle Snatching', 'Jewellery Shop Robbery',
                    'Robbery with Murder'
                )
                AND DATE(date) >= %(previous_start)s 
                AND DATE(date) < %(previous_end)s 
            THEN 1 ELSE 0 
            END) AS robbery_snatching_previous,
            SUM(CASE 
                WHEN level3_case_nature IN ('Rape', 'Child Abuse / Molestation')
                AND DATE(date) >= %(previous_start)s 
                AND DATE(date) < %(previous_end)s 
            THEN 1 ELSE 0 
            END) AS rape_sodomy_previous,
            SUM(CASE 
                WHEN level3_case_nature = 'Murder'
                AND DATE(date) >= %(previous_start)s 
                AND DATE(date) < %(previous_end)s 
            THEN 1 ELSE 0 
            END) AS murder_previous,
            SUM(CASE 
                WHEN level3_case_nature = 'Dacoity with Murder'
                AND DATE(date) >= %(previous_start)s 
                AND DATE(date) < %(previous_end)s 
            THEN 1 ELSE 0 
            END) AS dacoity_with_murder_previous
        FROM response_time
        WHERE police_station IS NOT NULL
          AND response_time IS NOT NULL
          AND DATE(date) >= %(previous_start)s
          AND DATE(date) < %(current_end)s
          AND parent_id = 0
          AND response_time > 0
          AND district_id NOT IN ('0', '41', '42', '43', '44', '45')
          AND district_id IS NOT NULL
        GROUP BY district_id, police_station;
    """


def get_minorities_query():
    return """
        SELECT 
            district_id,
            pucar_police_station,
            SUM(CASE 
                WHEN DATE(created_at) >= %s 
                     AND DATE(created_at) < %s 
                THEN 1 ELSE 0 
            END) AS minorities_current,
            SUM(CASE 
                WHEN DATE(created_at) >= %s 
                     AND DATE(created_at) < %s 
                THEN 1 ELSE 0 
            END) AS minorities_previous
        FROM case_final_status
        WHERE district_id IS NOT NULL
          AND pucar_police_station IS NOT NULL
          AND district_id NOT IN ('0', '41', '42', '43', '44', '45')
          AND DATE(created_at) >= %s
          AND DATE(created_at) < %s
        GROUP BY district_id, pucar_police_station;
    """


def get_category_fir_cases_query():
    return """
        SELECT 
            district_id,
            police_station,
            SUM(CASE WHEN DATE(date) >= %(current_start)s AND DATE(date) < %(current_end)s
                THEN dacoity ELSE 0 END) AS dacoity_fir_current,
            SUM(CASE WHEN DATE(date) >= %(previous_start)s AND DATE(date) < %(previous_end)s
                THEN dacoity ELSE 0 END) AS dacoity_fir_prior,
            SUM(CASE WHEN DATE(date) >= %(current_start)s AND DATE(date) < %(current_end)s
                THEN minorities ELSE 0 END) AS minorities_fir_current,
            SUM(CASE WHEN DATE(date) >= %(previous_start)s AND DATE(date) < %(previous_end)s
                THEN minorities ELSE 0 END) AS minorities_fir_prior,
            SUM(CASE WHEN DATE(date) >= %(current_start)s AND DATE(date) < %(current_end)s
                THEN burglary ELSE 0 END) AS burglary_fir_current,
            SUM(CASE WHEN DATE(date) >= %(previous_start)s AND DATE(date) < %(previous_end)s
                THEN burglary ELSE 0 END) AS burglary_fir_prior,
            SUM(CASE WHEN DATE(date) >= %(current_start)s AND DATE(date) < %(current_end)s
                THEN robbery_snatching ELSE 0 END) AS robbery_snatching_fir_current,
            SUM(CASE WHEN DATE(date) >= %(previous_start)s AND DATE(date) < %(previous_end)s
                THEN robbery_snatching ELSE 0 END) AS robbery_snatching_fir_prior,
            SUM(CASE WHEN DATE(date) >= %(current_start)s AND DATE(date) < %(current_end)s
                THEN murder ELSE 0 END) AS murder_fir_current,
            SUM(CASE WHEN DATE(date) >= %(previous_start)s AND DATE(date) < %(previous_end)s
                THEN murder ELSE 0 END) AS murder_fir_prior,
            SUM(CASE WHEN DATE(date) >= %(current_start)s AND DATE(date) < %(current_end)s
                THEN dacoity_with_murder ELSE 0 END) AS dacoity_with_murder_fir_current,
            SUM(CASE WHEN DATE(date) >= %(previous_start)s AND DATE(date) < %(previous_end)s
                THEN dacoity_with_murder ELSE 0 END) AS dacoity_with_murder_fir_prior,
            SUM(CASE WHEN DATE(date) >= %(current_start)s AND DATE(date) < %(current_end)s
                THEN (rape + child_abuse) ELSE 0 END) AS rape_sodomy_fir_current,
            SUM(CASE WHEN DATE(date) >= %(previous_start)s AND DATE(date) < %(previous_end)s
                THEN (rape + child_abuse) ELSE 0 END) AS rape_sodomy_fir_prior,
            SUM(CASE WHEN DATE(date) >= %(current_start)s AND DATE(date) < %(current_end)s
                THEN terrorist_act ELSE 0 END) AS terrorism_fir_current,
            SUM(CASE WHEN DATE(date) >= %(previous_start)s AND DATE(date) < %(previous_end)s
                THEN terrorist_act ELSE 0 END) AS terrorism_fir_prior
        FROM fir_cases
        WHERE district_id IS NOT NULL
          AND police_station IS NOT NULL
          AND district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
          AND DATE(date) >= %(previous_start)s
          AND DATE(date) < %(current_end)s
        GROUP BY district_id, police_station;
    """

def get_vccs_missing_query():
    return """
        SELECT 
            pucar_district_id,
            pucar_police_station,
            SUM(CASE 
                WHEN level3_case_nature IN ('Child Lost/ Missing', 'Missing Person reported')
                     AND DATE(created_at) >= %(current_start)s 
                     AND DATE(created_at) < %(current_end)s 
                THEN 1 ELSE 0 
            END) AS children_missing_current,
            SUM(CASE 
                WHEN is_lost_case = 1 AND is_handed_over = 1
                     AND DATE(created_at) >= %(current_start)s 
                     AND DATE(created_at) < %(current_end)s 
                THEN 1 ELSE 0 
            END) AS children_found_current,
            SUM(CASE 
                WHEN pucar_level3_case_nature_id IN (594, 595, 495, 493)
                     AND is_closed = 0
                     AND (is_handed_over IS NULL OR is_handed_over = 0)
                     AND DATE(created_at) >= %(current_start)s 
                     AND DATE(created_at) < %(current_end)s 
                THEN 1 ELSE 0 
            END) AS children_still_missing_current,
            SUM(CASE 
                WHEN level3_case_nature IN ('Child Lost/ Missing', 'Missing Person reported')
                     AND DATE(created_at) >= %(previous_start)s 
                     AND DATE(created_at) < %(previous_end)s 
                THEN 1 ELSE 0 
            END) AS children_missing_previous,
            SUM(CASE 
                WHEN is_lost_case = 1 AND is_handed_over = 1
                     AND DATE(created_at) >= %(previous_start)s 
                     AND DATE(created_at) < %(previous_end)s 
                THEN 1 ELSE 0 
            END) AS children_found_previous,
            SUM(CASE 
                WHEN pucar_level3_case_nature_id IN (594, 595, 495, 493)
                     AND is_closed = 0
                     AND (is_handed_over IS NULL OR is_handed_over = 0)
                     AND DATE(created_at) >= %(previous_start)s 
                     AND DATE(created_at) < %(previous_end)s 
                THEN 1 ELSE 0 
            END) AS children_still_missing_previous
        FROM case_final_status
        WHERE pucar_district_id IS NOT NULL
          AND pucar_police_station IS NOT NULL
          AND pucar_district_id NOT IN ('0', '41', '42', '43', '44', '45')
          AND DATE(created_at) >= %(previous_start)s
          AND DATE(created_at) < %(current_end)s
        GROUP BY pucar_district_id, pucar_police_station;
    """

def get_vwps_missing_query():
    return """
        SELECT 
            district_id,
            pucar_police_station,
            SUM(CASE 
                WHEN level3_case_nature = 'Missing Person reported'
                     AND DATE(created_at) >= %s 
                     AND DATE(created_at) < %s 
                THEN 1 ELSE 0 
            END) AS girls_missing_current,
            SUM(CASE 
                WHEN is_lost_case = 1 AND is_handed_over = 1
                     AND DATE(created_at) >= %s 
                     AND DATE(created_at) < %s 
                THEN 1 ELSE 0 
            END) AS girls_found_current,
            SUM(CASE 
                WHEN level3_case_nature = 'Missing Person reported'
                     AND DATE(created_at) >= %s 
                     AND DATE(created_at) < %s 
                THEN 1 ELSE 0 
            END) AS girls_missing_prior,
            SUM(CASE 
                WHEN is_lost_case = 1 AND is_handed_over = 1
                     AND DATE(created_at) >= %s 
                     AND DATE(created_at) < %s 
                THEN 1 ELSE 0 
            END) AS girls_found_prior
        FROM case_final_status
        WHERE district_id IS NOT NULL
          AND pucar_police_station IS NOT NULL
          AND district_id NOT IN ('0', '41', '42', '43', '44', '45')
          AND DATE(created_at) >= %s
          AND DATE(created_at) < %s
        GROUP BY district_id, pucar_police_station;
    """


def get_rt_alerts_query():
    return """
        SELECT 
            district_id,
            police_station,
            SUM(CASE 
                WHEN DATE(date) >= %(current_start)s 
                     AND DATE(date) < %(current_end)s 
                THEN 1 ELSE 0 
            END) AS current_period_count,
            SUM(CASE 
                WHEN DATE(date) >= %(previous_start)s 
                     AND DATE(date) < %(previous_end)s 
                THEN 1 ELSE 0 
            END) AS previous_period_count
        FROM response_time
        WHERE response_time > 2100
          AND parent_id = 0
          AND district_id IS NOT NULL
          AND police_station IS NOT NULL
          AND district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
          AND level3_case_nature NOT IN ('Other Help')
          AND level2_case_nature IN (
              'Robbery/Snatching', 'Burglary', 'Dacoity',
              'Sexual Assault', 'Kiddnapping / Abduction',
              'Murder', 'Terrorism Act'
          )
          AND DATE(date) >= %(previous_start)s
          AND DATE(date) < %(current_end)s
        GROUP BY district_id, police_station;
    """


def get_reoccurrence_query():
    return """
        SELECT 
            district,
            police_station,
            SUM(CASE 
                WHEN TO_DATE(date, 'DD-MM-YYYY') >= %(current_start)s 
                     AND TO_DATE(date, 'DD-MM-YYYY') < %(current_end)s 
                THEN 1 ELSE 0 
            END) AS previous_window_count,
            SUM(CASE 
                WHEN TO_DATE(date, 'DD-MM-YYYY') >= %(previous_start)s 
                     AND TO_DATE(date, 'DD-MM-YYYY') < %(previous_end)s 
                THEN 1 ELSE 0 
            END) AS before_previous_window_count
        FROM crime_hotspot
        WHERE case_number IS NOT NULL
          AND district IS NOT NULL
          AND police_station IS NOT NULL
          AND district NOT IN ('0', '41', '42', '43', '44', '45', '46')
          AND case_nature IN (
              'Any Other Dacoity', 'Any Other Robbery', 'Any Other Theft', 'Bank Burglary', 
              'Bank/Money Exchange/ ATM Dacoity', 'Bank/Money Exchange/ ATM Robbery', 
              'Car Snatching', 'Car Theft', 'Cattle Dacoity', 'Cattle Robbery', 'Cattle theft', 
              'Cycle Theft', 'Dacoity with Murder', 'Highway/Road/Street Dacoity', 
              'Highway/Road/Street Robbery', 'House Burglary', 'House Dacoity', 'House Robbery', 
              'Jewellery Shop Dacoity', 'Jewellery Shop Robbery', 'Mobile Theft', 'Motorcycle Snatching', 
              'Motorcycle Theft', 'Other Burglary', 'Other Vehicles Snatching', 'Other Vehicles Theft', 
              'Patrol Pump Dacoity', 'Patrol Pump Robbery', 'Pick Pocketing', 'Purse / Wallet / Luggage Theft', 
              'Robbery with Murder', 'Shop Burglary', 'Shop Dacoity', 'Shop Robbery', 'Snatching/Jhapatta', 
              'Transformer/ Motor Theft', 'Weapon Theft'
          )
          AND TO_DATE(date, 'DD-MM-YYYY') >= %(previous_start)s
          AND TO_DATE(date, 'DD-MM-YYYY') < %(current_end)s
        GROUP BY district, police_station;
    """


def get_rape_molestation_query():
    return """
        SELECT 
            district_id,
            police_station,
            SUM(CASE 
                WHEN level3_case_nature = 'Rape'
                     AND DATE(date) >= %(current_start)s 
                     AND DATE(date) < %(current_end)s 
                THEN 1 ELSE 0 
            END) AS rape_current,
            SUM(CASE 
                WHEN level3_case_nature = 'Child Abuse / Molestation'
                     AND DATE(date) >= %(current_start)s 
                     AND DATE(date) < %(current_end)s 
                THEN 1 ELSE 0 
            END) AS sodomy_current,
            SUM(CASE 
                WHEN level3_case_nature = 'Rape'
                     AND DATE(date) >= %(previous_start)s 
                     AND DATE(date) < %(previous_end)s 
                THEN 1 ELSE 0 
            END) AS rape_previous,
            SUM(CASE 
                WHEN level3_case_nature = 'Child Abuse / Molestation'
                     AND DATE(date) >= %(previous_start)s 
                     AND DATE(date) < %(previous_end)s 
                THEN 1 ELSE 0 
            END) AS sodomy_previous
        FROM response_time
        WHERE police_station IS NOT NULL
          AND response_time IS NOT NULL
          AND parent_id = 0
          AND response_time > 0
          AND district_id IS NOT NULL
          AND district_id NOT IN ('0', '41', '42', '43', '44', '45')
          AND DATE(date) >= %(previous_start)s
          AND DATE(date) < %(current_end)s
        GROUP BY district_id, police_station;
    """


def get_fir_cases_query():
    return """
        SELECT 
            district_id,
            police_station,
            SUM(CASE 
                WHEN DATE(date) >= %(current_start)s 
                     AND DATE(date) < %(current_end)s 
                THEN rape ELSE 0 
            END) AS rape_current,
            SUM(CASE 
                WHEN DATE(date) >= %(current_start)s 
                     AND DATE(date) < %(current_end)s 
                THEN child_abuse ELSE 0 
            END) AS child_abuse_current,
            SUM(CASE 
                WHEN DATE(date) >= %(previous_start)s 
                     AND DATE(date) < %(previous_end)s 
                THEN rape ELSE 0 
            END) AS rape_previous,
            SUM(CASE 
                WHEN DATE(date) >= %(previous_start)s 
                     AND DATE(date) < %(previous_end)s 
                THEN child_abuse ELSE 0 
            END) AS child_abuse_previous
        FROM fir_cases
        WHERE date IS NOT NULL
          AND district_id IS NOT NULL
          AND police_station IS NOT NULL
          AND district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
          AND DATE(date) >= %(previous_start)s
          AND DATE(date) < %(current_end)s
        GROUP BY district_id, police_station;
    """


def get_critical_issues_query():
    return """
        SELECT 
            district_id,
            police_station,
            SUM(CASE 
                WHEN level3_case_nature IN (
                    'Assault on Govt. Officials',
                    'Riot/Mob',
                    'Unlawful Assembly/Protest/Demonstration',
                    'Distribution/ Display of hateful sectarian material',
                    'Attack/Damage of Religious Places'
                )
                AND DATE(date) >= %s 
                AND DATE(date) < %s 
                THEN 1 ELSE 0 
            END) AS political_issues_current,
            SUM(CASE 
                WHEN level3_case_nature IN (
                    'Assault on Govt. Officials',
                    'Riot/Mob',
                    'Unlawful Assembly/Protest/Demonstration',
                    'Distribution/ Display of hateful sectarian material',
                    'Attack/Damage of Religious Places'
                )
                AND DATE(date) >= %s 
                AND DATE(date) < %s 
                THEN 1 ELSE 0 
            END) AS political_issues_previous,
            SUM(CASE 
                WHEN level3_case_nature IN (
                    'Complaint against PSCA',
                    'Hate Speech',
                    'Distribution/ Display of hateful sectarian material',
                    'Any Other Religious Issue'
                )
                AND DATE(date) >= %s 
                AND DATE(date) < %s 
                THEN 1 ELSE 0 
            END) AS media_related_issues_current,
            SUM(CASE 
                WHEN level3_case_nature IN (
                    'Complaint against PSCA',
                    'Hate Speech',
                    'Distribution/ Display of hateful sectarian material',
                    'Any Other Religious Issue'
                )
                AND DATE(date) >= %s 
                AND DATE(date) < %s 
                THEN 1 ELSE 0 
            END) AS media_related_issues_previous,
            SUM(CASE 
                WHEN level3_case_nature IN (
                    'Any Other Religious Issue',
                    'Attack/Damage of Religious Places',
                    'Defiling of Holy Book',
                    'Derogation of Holy Persons',
                    'Ehtram-e-Ramazan Ordinance 1981'
                )
                AND DATE(date) >= %s 
                AND DATE(date) < %s 
                THEN 1 ELSE 0 
            END) AS religious_issues_current,
            SUM(CASE 
                WHEN level3_case_nature IN (
                    'Any Other Religious Issue',
                    'Attack/Damage of Religious Places',
                    'Defiling of Holy Book',
                    'Derogation of Holy Persons',
                    'Ehtram-e-Ramazan Ordinance 1981'
                )
                AND DATE(date) >= %s 
                AND DATE(date) < %s 
                THEN 1 ELSE 0 
            END) AS religious_issues_previous,
            SUM(CASE 
                WHEN description ILIKE ANY (ARRAY['%%foreigner%%', '%%foriegner%%', '%%chinese%%'])
                AND DATE(date) >= %s 
                AND DATE(date) < %s 
                THEN 1 ELSE 0 
            END) AS foreign_current,
            SUM(CASE 
                WHEN description ILIKE ANY (ARRAY['%%foreigner%%', '%%foriegner%%', '%%chinese%%'])
                AND DATE(date) >= %s 
                AND DATE(date) < %s 
                THEN 1 ELSE 0 
            END) AS foreign_previous
        FROM response_time
        WHERE parent_id = 0
          AND district_id IS NOT NULL
          AND police_station IS NOT NULL
          AND district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
          AND DATE(date) >= %s
          AND DATE(date) < %s
        GROUP BY district_id, police_station;
    """

def get_complaints_category_query():
    return """
        SELECT
            complainant_district AS district_id,
            'UNKNOWN' AS police_station,
            -- Category 1: Non-FIR Registration
            SUM(CASE WHEN category = 1 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS non_fir_registration_pending_current,
            SUM(CASE WHEN category = 1 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue', 'Closed (Disposed)')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS non_fir_registration_total_current,
            SUM(CASE WHEN category = 1 AND complaint_status = 'Closed (Disposed)'
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS non_fir_registration_completed_current,
            SUM(CASE WHEN category = 1 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS non_fir_registration_pending_prior,
            SUM(CASE WHEN category = 1 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue', 'Closed (Disposed)')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS non_fir_registration_total_prior,
            SUM(CASE WHEN category = 1 AND complaint_status = 'Closed (Disposed)'
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS non_fir_registration_completed_prior,
            -- Category 2: Under Investigation
            SUM(CASE WHEN category = 2 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS under_investigation_pending_current,
            SUM(CASE WHEN category = 2 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue', 'Closed (Disposed)')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS under_investigation_total_current,
            SUM(CASE WHEN category = 2 AND complaint_status = 'Closed (Disposed)'
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS under_investigation_completed_current,
            SUM(CASE WHEN category = 2 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS under_investigation_pending_prior,
            SUM(CASE WHEN category = 2 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue', 'Closed (Disposed)')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS under_investigation_total_prior,
            SUM(CASE WHEN category = 2 AND complaint_status = 'Closed (Disposed)'
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS under_investigation_completed_prior,
            -- Category 3: Complaint Against Police
            SUM(CASE WHEN category = 3 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS complaint_against_police_pending_current,
            SUM(CASE WHEN category = 3 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue', 'Closed (Disposed)')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS complaint_against_police_total_current,
            SUM(CASE WHEN category = 3 AND complaint_status = 'Closed (Disposed)'
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS complaint_against_police_completed_current,
            SUM(CASE WHEN category = 3 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS complaint_against_police_pending_prior,
            SUM(CASE WHEN category = 3 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue', 'Closed (Disposed)')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS complaint_against_police_total_prior,
            SUM(CASE WHEN category = 3 AND complaint_status = 'Closed (Disposed)'
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS complaint_against_police_completed_prior,
            -- Category 4: Complaint Against Services
            SUM(CASE WHEN category = 4 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS complaint_against_services_pending_current,
            SUM(CASE WHEN category = 4 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue', 'Closed (Disposed)')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS complaint_against_services_total_current,
            SUM(CASE WHEN category = 4 AND complaint_status = 'Closed (Disposed)'
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS complaint_against_services_completed_current,
            SUM(CASE WHEN category = 4 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS complaint_against_services_pending_prior,
            SUM(CASE WHEN category = 4 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue', 'Closed (Disposed)')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS complaint_against_services_total_prior,
            SUM(CASE WHEN category = 4 AND complaint_status = 'Closed (Disposed)'
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS complaint_against_services_completed_prior,
            -- Category 5: Departmental Issue
            SUM(CASE WHEN category = 5 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS departmental_issue_pending_current,
            SUM(CASE WHEN category = 5 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue', 'Closed (Disposed)')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS departmental_issue_total_current,
            SUM(CASE WHEN category = 5 AND complaint_status = 'Closed (Disposed)'
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(current_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
                THEN 1 ELSE 0 END) AS departmental_issue_completed_current,
            SUM(CASE WHEN category = 5 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS departmental_issue_pending_prior,
            SUM(CASE WHEN category = 5 AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue', 'Closed (Disposed)')
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS departmental_issue_total_prior,
            SUM(CASE WHEN category = 5 AND complaint_status = 'Closed (Disposed)'
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
                     AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(previous_end)s
                THEN 1 ELSE 0 END) AS departmental_issue_completed_prior
        FROM complaints_view
        WHERE source = 2
          AND complainant_district IS NOT NULL
          AND complainant_district NOT IN ('0', '41', '42', '43', '44', '45')
          AND complaint_date IS NOT NULL
          AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) >= %(previous_start)s
          AND DATE(FROM_UNIXTIME(COALESCE(complaint_date, 0))) < %(current_end)s
          AND complaint_status IN ('Pending (Fresh)', 'In Proceeding', 'Pending (Reopened)', 'Overdue', 'Closed (Disposed)')
        GROUP BY complainant_district;
    """

def get_pkm_service_query():
    return """
        SELECT 
            district_id,
            'UNKNOWN' AS police_station,
            service_name,
            SUM(CASE 
                WHEN date >= %(current_start)s AND date < %(current_end)s  
                THEN pending ELSE 0 
            END) AS pending_current,
            SUM(CASE 
                WHEN date >= %(previous_start)s  AND date < %(previous_end)s  
                THEN pending ELSE 0 
            END) AS pending_previous,
            SUM(CASE 
                WHEN date >= %(current_start)s AND date < %(current_end)s 
                THEN complete ELSE 0 
            END) AS complete_current,
            SUM(CASE 
                WHEN date >= %(previous_start)s  AND date < %(previous_end)s 
                THEN complete ELSE 0 
            END) AS complete_previous,
            SUM(CASE 
                WHEN date >= %(current_start)s AND date < %(current_end)s  
                THEN inprogress ELSE 0 
            END) AS inprogress_current,
            SUM(CASE 
                WHEN date >= %(previous_start)s AND date < %(previous_end)s 
                THEN inprogress ELSE 0 
            END) AS inprogress_previous
        FROM pkm_data
        WHERE date >= %(previous_start)s
          AND date < %(current_end)s
          AND district_id IS NOT NULL
          AND district_id NOT IN ('0', '41', '42', '43', '44', '45', '46')
        GROUP BY district_id, service_name;
    """


def process_pkm_service_data(cursor, period, current_start, current_end, previous_start, previous_end):
    query = get_pkm_service_query()
    params = {
        'current_start': current_start,
        'current_end': current_end,
        'previous_start': previous_start,
        'previous_end': previous_end
    }
    cursor.execute(query, params)
    rows = cursor.fetchall()
    data = []
    for row in rows:
        district_id, police_station, service_name, pending_current, \
            pending_previous, complete_current, complete_previous, \
            inprogress_current, inprogress_previous = row

        # Define metrics and their counts
        metrics = [
            (f'pending_{service_name.lower().replace(" ", "_")}', pending_current, pending_previous),
            (f'complete_{service_name.lower().replace(" ", "_")}', complete_current, complete_previous),
            (f'inprogress_{service_name.lower().replace(" ", "_")}', inprogress_current, inprogress_previous)
        ]

        for metric_name, current_count, previous_count in metrics:
            percentage_change = (
                ((current_count - previous_count) / previous_count * 100)
                if previous_count > 0 else 0
            )
            data.append((
                metric_name, period, district_id, police_station,
                current_start, current_end, previous_start, previous_end,
                current_count, previous_count, percentage_change, datetime.now()
            ))

    return data


def process_1787_complaint_data(cursor, period, current_start, current_end, previous_start, previous_end):
    try:
        query = get_complaints_category_query()
        params = {
            'current_start': current_start,
            'current_end': current_end,
            'previous_start': previous_start,
            'previous_end': previous_end
        }
        logging.debug(f"Executing complaints_category_query with params: {params}")
        cursor.execute(query, params)
        rows = cursor.fetchall()
        data = []
        for row in rows:
            (district_id, police_station,
             non_fir_registration_pending_current, non_fir_registration_total_current, non_fir_registration_completed_current,
             non_fir_registration_pending_prior, non_fir_registration_total_prior, non_fir_registration_completed_prior,
             under_investigation_pending_current, under_investigation_total_current, under_investigation_completed_current,
             under_investigation_pending_prior, under_investigation_total_prior, under_investigation_completed_prior,
             complaint_against_police_pending_current, complaint_against_police_total_current, complaint_against_police_completed_current,
             complaint_against_police_pending_prior, complaint_against_police_total_prior, complaint_against_police_completed_prior,
             complaint_against_services_pending_current, complaint_against_services_total_current, complaint_against_services_completed_current,
             complaint_against_services_pending_prior, complaint_against_services_total_prior, complaint_against_services_completed_prior,
             departmental_issue_pending_current, departmental_issue_total_current, departmental_issue_completed_current,
             departmental_issue_pending_prior, departmental_issue_total_prior, departmental_issue_completed_prior) = row

            # Define metrics and their counts
            metrics = [
                ('non_fir_registration_pending', non_fir_registration_pending_current or 0, non_fir_registration_pending_prior or 0),
                ('non_fir_registration_total', non_fir_registration_total_current or 0, non_fir_registration_total_prior or 0),
                ('non_fir_registration_completed', non_fir_registration_completed_current or 0, non_fir_registration_completed_prior or 0),
                ('under_investigation_pending', under_investigation_pending_current or 0, under_investigation_pending_prior or 0),
                ('under_investigation_total', under_investigation_total_current or 0, under_investigation_total_prior or 0),
                ('under_investigation_completed', under_investigation_completed_current or 0, under_investigation_completed_prior or 0),
                ('complaint_against_police_pending', complaint_against_police_pending_current or 0, complaint_against_police_pending_prior or 0),
                ('complaint_against_police_total', complaint_against_police_total_current or 0, complaint_against_police_total_prior or 0),
                ('complaint_against_police_completed', complaint_against_police_completed_current or 0, complaint_against_police_completed_prior or 0),
                ('complaint_against_services_pending', complaint_against_services_pending_current or 0, complaint_against_services_pending_prior or 0),
                ('complaint_against_services_total', complaint_against_services_total_current or 0, complaint_against_services_total_prior or 0),
                ('complaint_against_services_completed', complaint_against_services_completed_current or 0, complaint_against_services_completed_prior or 0),
                ('departmental_issue_pending', departmental_issue_pending_current or 0, departmental_issue_pending_prior or 0),
                ('departmental_issue_total', departmental_issue_total_current or 0, departmental_issue_total_prior or 0),
                ('departmental_issue_completed', departmental_issue_completed_current or 0, departmental_issue_completed_prior or 0)
            ]

            for metric_name, current_count, previous_count in metrics:
                percentage_change = (
                    ((current_count - previous_count) / previous_count * 100)
                    if previous_count > 0 else 0
                )
                data.append((
                    metric_name, period, district_id, police_station,
                    current_start, current_end, previous_start, previous_end,
                    current_count, previous_count, percentage_change, datetime.now()
                ))
        logging.debug(f"Processed {len(data)} rows for complaints_category_data")
        return data
    except MySQLError as e:
        logging.error(f"MySQL error in process_complaints_category_data: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"General error in process_complaints_category_data: {str(e)}", exc_info=True)
        raise

def process_critical_issues_data(cursor, period, current_start, current_end, previous_start, previous_end):
    query = get_critical_issues_query()
    params = (
            current_start, current_end,  # political_issues_current
            previous_start, previous_end,  # political_issues_previous
            current_start, current_end,  # media_related_issues_current
            previous_start, previous_end,  # media_related_issues_previous
            current_start, current_end,  # religious_issues_current
            previous_start, previous_end,  # religious_issues_previous
            current_start, current_end,  # foreign_current
            previous_start, previous_end,  # foreign_previous
            previous_start, current_end   # WHERE clause
        )
    cursor.execute(query, params)
    rows = cursor.fetchall()
    data = []
    for row in rows:
        (district_id, police_station,
         political_issues_current, political_issues_previous,
         media_related_issues_current, media_related_issues_previous,
         religious_issues_current, religious_issues_previous,
         foreign_current, foreign_previous) = row

        # Define metrics and their counts
        metrics = [
            ('political_issues', political_issues_current, political_issues_previous),
            ('media_related_issues', media_related_issues_current, media_related_issues_previous),
            ('religious_issues', religious_issues_current, religious_issues_previous),
            ('foreign_cases', foreign_current, foreign_previous)
        ]

        for metric_name, current_count, previous_count in metrics:
            percentage_change = (
                ((current_count - previous_count) / previous_count * 100)
                if previous_count > 0 else 0
            )
            data.append((
                metric_name, period, district_id, police_station,
                current_start, current_end, previous_start, previous_end,
                current_count, previous_count, percentage_change, datetime.now()
            ))

    return data


def process_rape_molestation_fir_data(cursor, period, current_start, current_end, previous_start, previous_end):
    query = get_fir_cases_query()
    params = {
        'current_start': current_start,
        'current_end': current_end,
        'previous_start': previous_start,
        'previous_end': previous_end
    }
    cursor.execute(query, params)
    rows = cursor.fetchall()
    data = []
    for row in rows:
        district_id, police_station, rape_current, child_abuse_current, rape_previous, child_abuse_previous = row

        # Define metrics and their counts
        metrics = [
            ('fir_rape', rape_current, rape_previous),
            ('fir_child_abuse', child_abuse_current, child_abuse_previous)
        ]

        for metric_name, current_count, previous_count in metrics:
            percentage_change = (
                ((current_count - previous_count) / previous_count * 100)
                if previous_count > 0 else 0
            )
            data.append((
                metric_name, period, district_id, police_station,
                current_start, current_end, previous_start, previous_end,
                current_count, previous_count, percentage_change, datetime.now()
            ))

    return data

def process_vccs_missing_data(cursor, period, current_start, current_end, previous_start, previous_end):
    try:
        query = get_vccs_missing_query()
        params = {
            'current_start': current_start,
            'current_end': current_end,
            'previous_start': previous_start,
            'previous_end': previous_end
        }
        cursor.execute(query, params)
        rows = cursor.fetchall()
        data = []
        for row in rows:
            (district_id, police_station,
             children_missing_current, children_found_current, children_still_missing_current,
             children_missing_previous, children_found_previous, children_still_missing_previous) = row

            # Define metrics and their counts
            metrics = [
                ('children_missing', children_missing_current, children_missing_previous),
                ('children_found', children_found_current, children_found_previous),
                ('children_still_missing', children_still_missing_current, children_still_missing_previous)
            ]

            for metric_name, current_count, previous_count in metrics:
                percentage_change = (
                    ((current_count - previous_count) / previous_count * 100)
                    if previous_count > 0 else 0
                )
                data.append((
                    metric_name, period, district_id, police_station,
                    current_start, current_end, previous_start, previous_end,
                    current_count, previous_count, percentage_change, datetime.now()
                ))
        logging.debug(f"Processed {len(data)} rows for vccs_missing_data")
        return data

    except MySQLError as e:
        logging.error(f"MySQL error in process_vccs_missing_data: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"General error in process_vccs_missing_data: {str(e)}", exc_info=True)
        raise

def process_vwps_missing_data(cursor, period, current_start, current_end, previous_start, previous_end):
    try:
        query = get_vwps_missing_query()
        params = (
            current_start, current_end,  # girls_missing_current
            current_start, current_end,  # girls_found_current
            previous_start, previous_end,  # girls_missing_prior
            previous_start, previous_end,  # girls_found_prior
            previous_start, current_end   # WHERE clause
        )
        logging.debug(f"Executing vwps_missing_query with params: {params}")
        cursor.execute(query, params)
        rows = cursor.fetchall()
        data = []
        for row in rows:
            (district_id, police_station,
             girls_missing_current, girls_found_current,
             girls_missing_prior, girls_found_prior) = row

            # Replace NULL police_station with 'unknown'
            police_station = police_station or 'unknown'

            # Define metrics and their counts
            metrics = [
                ('girls_missing', girls_missing_current, girls_missing_prior),
                ('girls_found', girls_found_current, girls_found_prior)
            ]

            for metric_name, current_count, previous_count in metrics:
                percentage_change = (
                    ((current_count - previous_count) / previous_count * 100)
                    if previous_count > 0 else 0
                )
                data.append((
                    metric_name, period, district_id, police_station,
                    current_start, current_end, previous_start, previous_end,
                    current_count, previous_count, percentage_change, datetime.now()
                ))
        logging.debug(f"Processed {len(data)} rows for vwps_missing_data")
        return data
    except MySQLError as e:
        logging.error(f"MySQL error in process_vwps_missing_data: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"General error in process_vwps_missing_data: {str(e)}", exc_info=True)
        raise

# Process conference call data
def process_conference_call_data(cursor, period, current_start, current_end, previous_start, previous_end):
    query = get_conference_call_query()
    params = {
        'current_start': current_start,
        'current_end': current_end,
        'previous_start': previous_start,
        'previous_end': previous_end
    }
    cursor.execute(query, params)
    rows = cursor.fetchall()
    data = []
    for row in rows:
        district_id, police_station, current_success_count, previous_success_count, current_total_count, previous_total_count = row
        # Successful conference call
        percentage_change_success = (
            ((current_success_count - previous_success_count) / previous_success_count * 100)
            if previous_success_count > 0 else 0
        )
        data.append((
            'successful_conference_call', period, district_id, police_station,
            current_start, current_end, previous_start, previous_end,
            current_success_count, previous_success_count, percentage_change_success, datetime.now()
        ))
        # Total conference call
        percentage_change_total = (
            ((current_total_count - previous_total_count) / previous_total_count * 100)
            if previous_total_count > 0 else 0
        )
        data.append((
            'total_conference_call', period, district_id, police_station,
            current_start, current_end, previous_start, previous_end,
            current_total_count, previous_total_count, percentage_change_total, datetime.now()
        ))
    return data

# Process negative feedback data
def process_negative_feedback_data(cursor, period, current_start, current_end, previous_start, previous_end):
    query = get_negative_feedback_query()
    params = {
        'current_start': current_start,
        'current_end': current_end,
        'previous_start': previous_start,
        'previous_end': previous_end
    }
    cursor.execute(query, params)
    rows = cursor.fetchall()
    data = []
    for row in rows:
        district_id, police_station, current_negative_count, previous_negative_count = row
        percentage_change = (
            ((current_negative_count - previous_negative_count) / previous_negative_count * 100)
            if previous_negative_count > 0 else 0
        )
        data.append((
            'negative_caller_feedback', period, district_id, police_station,
            current_start, current_end, previous_start, previous_end,
            current_negative_count, previous_negative_count, percentage_change, datetime.now()
        ))
    return data


def process_category_data(cursor, period, current_start, current_end, previous_start, previous_end):
    query = get_dashboard_categories_query()
    params = {
        'current_start': current_start,
        'current_end': current_end,
        'previous_start': previous_start,
        'previous_end': previous_end
    }
    cursor.execute(query, params)
    rows = cursor.fetchall()
    data = []
    for row in rows:
        (district_id, police_station,
         terrorism_current, dacoity_current, robbery_snatching_current,
         rape_sodomy_current, murder_current, dacoity_with_murder_current,
         terrorism_previous, dacoity_previous, robbery_snatching_previous,
         rape_sodomy_previous, murder_previous, dacoity_with_murder_previous) = row

        # Define metrics and their counts
        metrics = [
            ('terrorism', terrorism_current, terrorism_previous),
            ('dacoity', dacoity_current, dacoity_previous),
            ('robbery_snatching', robbery_snatching_current, robbery_snatching_previous),
            ('rape_sodomy', rape_sodomy_current, rape_sodomy_previous),
            ('murder', murder_current, murder_previous),
            ('dacoity_with_murder', dacoity_with_murder_current, dacoity_with_murder_previous)
        ]

        for metric_name, current_count, previous_count in metrics:
            percentage_change = (
                ((current_count - previous_count) / previous_count * 100)
                if previous_count > 0 else 0
            )
            data.append((
                metric_name, period, district_id, police_station,
                current_start, current_end, previous_start, previous_end,
                current_count, previous_count, percentage_change, datetime.now()
            ))

    return data

def process_rt_alerts_data(cursor, period, current_start, current_end, previous_start, previous_end):
    query = get_rt_alerts_query()
    params = {
        'current_start': current_start,
        'current_end': current_end,
        'previous_start': previous_start,
        'previous_end': previous_end
    }
    cursor.execute(query, params)
    rows = cursor.fetchall()
    data = []
    for row in rows:
        district_id, police_station, current_count, previous_count = row
        percentage_change = (
            ((current_count - previous_count) / previous_count * 100)
            if previous_count > 0 else 0
        )
        data.append((
            'rt_alerts', period, district_id, police_station,
            current_start, current_end, previous_start, previous_end,
            current_count, previous_count, percentage_change, datetime.now()
        ))
    return data


def process_minorities_data(cursor, period, current_start, current_end, previous_start, previous_end):
    try:
        query = get_minorities_query()
        params = (
            current_start, current_end,  # minorities_current
            previous_start, previous_end,  # minorities_previous
            previous_start, current_end   # WHERE clause
        )
        logging.debug(f"Executing minorities_query with params: {params}")
        cursor.execute(query, params)
        rows = cursor.fetchall()
        data = []
        for row in rows:
            district_id, police_station, minorities_current, minorities_previous = row

            # Replace NULL police_station with 'unknown'
            police_station = police_station or 'unknown'

            # Define metric
            metric_name = 'minorities'
            current_count = minorities_current
            previous_count = minorities_previous
            percentage_change = (
                ((current_count - previous_count) / previous_count * 100)
                if previous_count > 0 else 0
            )
            data.append((
                metric_name, period, district_id, police_station,
                current_start, current_end, previous_start, previous_end,
                current_count, previous_count, percentage_change, datetime.now()
            ))
        logging.debug(f"Processed {len(data)} rows for minorities_data")
        return data
    except MySQLError as e:
        logging.error(f"MySQL error in process_minorities_data: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"General error in process_minorities_data: {str(e)}", exc_info=True)
        raise


def process_fir_cases_data(cursor, period, current_start, current_end, previous_start, previous_end):
    try:
        query = get_category_fir_cases_query()
        params = {
            'current_start': current_start,
            'current_end': current_end,
            'previous_start': previous_start,
            'previous_end': previous_end
        }
        logging.debug(f"Executing fir_cases_query with params: {params}")
        cursor.execute(query, params)
        rows = cursor.fetchall()
        data = []
        for row in rows:
            (district_id, police_station,
             dacoity_fir_current, dacoity_fir_prior,
             minorities_fir_current, minorities_fir_prior,
             burglary_fir_current, burglary_fir_prior,
             robbery_snatching_fir_current, robbery_snatching_fir_prior,
             murder_fir_current, murder_fir_prior,
             dacoity_with_murder_fir_current, dacoity_with_murder_fir_prior,
             rape_sodomy_fir_current, rape_sodomy_fir_prior,
             terrorism_fir_current, terrorism_fir_prior) = row

            # Replace NULL police_station with 'unknown' (for safety)
            police_station = police_station or 'unknown'

            # Define metrics and their counts
            metrics = [
                ('dacoity_fir', dacoity_fir_current or 0, dacoity_fir_prior or 0),
                ('minorities_fir', minorities_fir_current or 0, minorities_fir_prior or 0),
                ('burglary_fir', burglary_fir_current or 0, burglary_fir_prior or 0),
                ('robbery_snatching_fir', robbery_snatching_fir_current or 0, robbery_snatching_fir_prior or 0),
                ('murder_fir', murder_fir_current or 0, murder_fir_prior or 0),
                ('dacoity_with_murder_fir', dacoity_with_murder_fir_current or 0, dacoity_with_murder_fir_prior or 0),
                ('rape_sodomy_fir', rape_sodomy_fir_current or 0, rape_sodomy_fir_prior or 0),
                ('terrorism_fir', terrorism_fir_current or 0, terrorism_fir_prior or 0)
            ]

            for metric_name, current_count, previous_count in metrics:
                percentage_change = (
                    ((current_count - previous_count) / previous_count * 100)
                    if previous_count > 0 else 0
                )
                data.append((
                    metric_name, period, district_id, police_station,
                    current_start, current_end, previous_start, previous_end,
                    current_count, previous_count, percentage_change, datetime.now()
                ))
        logging.debug(f"Processed {len(data)} rows for fir_cases_data")
        return data
    except MySQLError as e:
        logging.error(f"MySQL error in process_fir_cases_data: {str(e)}", exc_info=True)
        raise
    except Exception as e:
        logging.error(f"General error in process_fir_cases_data: {str(e)}", exc_info=True)
        raise


def process_reoccurrence_data(cursor, period, current_start, current_end, previous_start, previous_end):
    query = get_reoccurrence_query()
    params = {
        'current_start': current_start,
        'current_end': current_end,
        'previous_start': previous_start,
        'previous_end': previous_end
    }
    cursor.execute(query, params)
    rows = cursor.fetchall()
    data = []
    for row in rows:
        district_id, police_station, current_count, previous_count = row
        percentage_change = (
            ((current_count - previous_count) / previous_count * 100)
            if previous_count > 0 else 0
        )
        data.append((
            'crime_reoccurrence', period, district_id, police_station,
            current_start, current_end, previous_start, previous_end,
            current_count, previous_count, percentage_change, datetime.now()
        ))
    return data


def process_rape_molestation_data(cursor, period, current_start, current_end, previous_start, previous_end):
    query = get_rape_molestation_query()
    params = {
        'current_start': current_start,
        'current_end': current_end,
        'previous_start': previous_start,
        'previous_end': previous_end
    }
    cursor.execute(query, params)
    rows = cursor.fetchall()
    data = []
    for row in rows:
        district_id, police_station, rape_current, sodomy_current, rape_previous, sodomy_previous = row

        # Define metrics and their counts
        metrics = [
            ('rape_cases', rape_current, rape_previous),
            ('sodomy_cases', sodomy_current, sodomy_previous)
        ]

        for metric_name, current_count, previous_count in metrics:
            percentage_change = (
                ((current_count - previous_count) / previous_count * 100)
                if previous_count > 0 else 0
            )
            data.append((
                metric_name, period, district_id, police_station,
                current_start, current_end, previous_start, previous_end,
                current_count, previous_count, percentage_change, datetime.now()
            ))

    return data


# Insert data into igp_insights
def insert_data(conn, data):
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO igp_insights (
                metric_name, period_type, district_id, police_station,
                current_start_date, current_end_date, previous_start_date, previous_end_date,
                current_count, previous_count, percentage_change, computed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (metric_name, period_type, district_id, police_station) DO UPDATE SET
                current_start_date = EXCLUDED.current_start_date,
                current_end_date = EXCLUDED.current_end_date,
                previous_start_date = EXCLUDED.previous_start_date,
                previous_end_date = EXCLUDED.previous_end_date,
                current_count = EXCLUDED.current_count,
                previous_count = EXCLUDED.previous_count,
                percentage_change = EXCLUDED.percentage_change,
                computed_at = EXCLUDED.computed_at;
        """, data)
    conn.commit()
    logging.info(f"Inserted {len(data)} rows into igp_insights.")


def main():
    today = datetime.today().date() - timedelta(days=1)

    # Connect to databases
    try:
        logging.debug("Initializing database connections...")
        source_conn = get_processed_db_connection()
        logging.debug("PostgreSQL source_conn established")
        mysql_conn = get_1787_db_connection()
        logging.debug("MySQL mysql_conn established")
        vccs_conn, vwps_conn, vcm_conn = connect_to_mysql()
        logging.debug("MySQL vccs_conn established")
        predictive_conn = get_processed_db_connection(database={
            'dbname': 'db_predictive_policing',
            'user': 'postgres',
            'password': 'psca@officialmai1',
            'host': '10.20.170.151',
            'port': 5432
        })
    except Exception as e:
        logging.error(f"Failed to establish database connections: {str(e)}", exc_info=True)
        return

    # Ensure igp_insights table exists
    try:
        create_tables(source_conn)
        logging.debug("igp_insights table ensured")
    except Exception as e:
        logging.error(f"Error creating tables: {str(e)}", exc_info=True)
        return

    # List of event processors with their respective connections
    event_processors = [
        (process_fir_cases_data, source_conn),  # PostgreSQL
        (process_critical_issues_data, source_conn),
        (process_1787_complaint_data, mysql_conn),  # Mysql
        (process_vccs_missing_data, vccs_conn),  # VCCS MySQL
        (process_vwps_missing_data, vwps_conn),   # VWPS MYSQL
        (process_minorities_data, vcm_conn), # VCM MYSQL
        (process_pkm_service_data, source_conn), # PostgreSQL
        (process_conference_call_data, source_conn),  # PostgreSQL
        (process_negative_feedback_data, source_conn),  # PostgreSQL
        (process_category_data, source_conn),  # PostgreSQL
        (process_rt_alerts_data, source_conn),  # PostgreSQL
        (process_reoccurrence_data, predictive_conn),  # PostgreSQL
        (process_rape_molestation_data, source_conn),  # PostgreSQL
        (process_rape_molestation_fir_data, source_conn)  # PostgreSQL
        # PostgreSQL
    ]

    # Process all events for each period
    all_data = []
    for period in PERIODS.keys():
        try:
            current_start, current_end, previous_start, previous_end = calculate_dates(period, today)
            logging.info(f"Processing period: {period}")

            for processor, conn in event_processors:
                processor_name = processor.__name__
                cursor = None

                try:
                    # Validate MySQL connection
                    if isinstance(conn, mysql.connector.connection.MySQLConnection):
                        if not conn.is_connected():
                            logging.warning(f"MySQL connection for {processor_name} is closed. Reconnecting...")
                            conn.reconnect(attempts=3, delay=1)
                            if not conn.is_connected():
                                raise MySQLError(f"Failed to reconnect MySQL for {processor_name}")
                        logging.debug(f"MySQL connection for {processor_name} is valid")
                        cursor = conn.cursor(buffered=True)  # Manual cursor for MySQL
                    # Validate PostgreSQL connection
                    elif isinstance(conn, psycopg2.extensions.connection):
                        if conn.closed:
                            logging.warning(f"PostgreSQL connection for {processor_name} is closed. Reconnecting...")
                            if processor_name == 'process_reoccurrence_data':
                                conn = get_processed_db_connection(database={
                                    'dbname': 'db_predictive_policing',
                                    'user': 'postgres',
                                    'password': 'psca@officialmai1',
                                    'host': '10.20.170.151',
                                    'port': 5432
                                })
                            else:
                                conn = get_processed_db_connection()
                            if conn.closed:
                                raise psycopg2.Error(f"Failed to reconnect PostgreSQL for {processor_name}")
                        logging.debug(f"PostgreSQL connection for {processor_name} is valid")
                        cursor = conn.cursor()  # PostgreSQL cursors can use context manager, but we'll keep it consistent

                    logging.debug(f"Executing processor: {processor_name} for period: {period}")
                    data = processor(cursor, period, current_start, current_end, previous_start, previous_end)
                    all_data.extend(data)
                    logging.debug(f"Processor {processor_name} completed with {len(data)} rows")
                except Exception as e:
                    logging.error(f"Error in processor {processor_name} for period {period}: {str(e)}", exc_info=True)
                    continue  # Continue with next processor
                finally:
                    if cursor:
                        try:
                            cursor.close()
                            logging.debug(f"Cursor closed for {processor_name}")
                        except Exception as e:
                            logging.error(f"Error closing cursor for {processor_name}: {str(e)}", exc_info=True)
        except Exception as e:
            logging.error(f"Error processing period {period}: {str(e)}", exc_info=True)
            continue

    # Insert all data
    try:
        if all_data:
            insert_data(source_conn, all_data)
            logging.info(f"Inserted {len(all_data)} rows into igp_insights Table")
    except Exception as e:
        logging.error(f"Error inserting data: {str(e)}", exc_info=True)

    # Clean up
    try:
        source_conn.close()
        vccs_conn.close()
        predictive_conn.close()
        mysql_conn.close()
        logging.debug("All database connections closed")
    except Exception as e:
        logging.error(f"Error closing connections: {str(e)}", exc_info=True)

    except Exception as e:
        logging.critical(f"Pipeline failed: {str(e)}")
        raise  # Re-raise to allow external monitoring (e.g., cron) to detect failure

    finally:
        # Clean up database connections
        for conn, conn_name in [(source_conn, "source_conn"), (mysql_conn, "mysql_conn"), (vccs_conn, "vccs_conn"), (predictive_conn, "predictive_conn")]:
            if conn is not None:
                try:
                    conn.close()
                    logging.info(f"Closed {conn_name} connection.")
                except Exception as e:
                    logging.error(f"Failed to close {conn_name} connection: {str(e)}")

if __name__ == "__main__":
    main()