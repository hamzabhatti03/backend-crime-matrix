"""PROJECT CONFIGURATIONS"""
from datetime import timedelta
import os
from dotenv import load_dotenv

load_dotenv()

""""MAIN SERVER CONFIGURATIONS"""
PORT = 5010
DEBUG_ = True
HOST = '0.0.0.0'
ONE_TIME_RUN = True

"""ALLOWED IPs for API REQUESTS"""
WHITE_LISTED_IPS = ['10.22.15.235', '10.20.170.151', '10.20.170.219', '10.20.170.232', '10.22.15.119', '10.20.12.146',
                    '10.20.12.157', '10.21.63.149', '10.22.16.245']  # '10.22.15.91'

"""API REQUEST LIMITER"""
DEFAULT_LIMITER = ["50000 per day", "5000 per hour"]
LIMITER = "750 per minute"

"""CACHE CONFIGS"""
CACHE_CONFIGS = {'CACHE_TYPE': 'simple'}

"""LOG FILE PATH"""
API_DETAILS_LOG_PATH = '../Logs/api_details.log'
PROCESSING_STATS_LOG_PATH = '../Logs/processing_stats.log'

"""DEFAULT DISTRICT(LAHORE)"""
DEFAULT_DISTRICT_ID = '40'

"""MAIN DATABASES"""
PROCESSING_DATA_MASTER = '../DatabaseManager/processing_data_master.db'
PROCESSED_STATS_MAIN = '../DatabaseManager/processed.db'
POLICE_STATIONS_MAIN = r'../DatabaseManager/police_stations.db'
CCM_AGENTS_MAIN = '../DatabaseManager/ccm_agents_13082024.db'
FIR_DB = '../DatabaseManager/leads_in_fir_14112024.db'
PROCESSED_STATS_TEST_DB = '../DatabaseManager/processed1.db'

"""PostgreSQL DATABASES"""
POSTGRES_PROCESSED_STATS_MAIN = {
    'dbname': 'test_1124',
    'user': 'postgres',
    'password': 'psca@officialmai1',
    'host': '10.20.170.151',
    'port': 5432  # Default PostgreSQL port
}

"""STAGING DATABASES"""
CMS_STAGING = '../DatabaseManager/cms_staging.db'
LEADS_IN_FOR_CMS_STAGING = '../DatabaseManager/leads_in_all_districts1.db'
# FIR_DB = '../DatabaseManager/leads_in_fir_15102024.db'

"""DATABASE TABLES"""
WOMEN_SAFETY_TABLE = "women_safety_api_logs"
OUTGOING_CALL_LOGS_TABLE = "outgoing_calls_logs"
CALL_FEEDBACK_AGENT = "call_feedback_agent"
VIDEO_CALLS_TABLE = "video_calls"
FIELD_LIVE_LOCATION_TABLE = "field_live_location"
DUTIES_TABLE = "duties"
LEADS_IN_TABLE = "leads_in"

"""API ENDPOINTS AND METHODS"""
CASE_STATS = {'ENDPOINT': '/case_stats', 'METHOD': 'GET'}
AVG_RESPONSE_TIME = {'ENDPOINT': '/avg_response_time', 'METHOD': 'GET'}
DIST_RESPONSE_TIME = {'ENDPOINT': '/districtwise_response_time', 'METHOD': 'GET'}
DATEWISE_STATS = {'ENDPOINT': '/datewise_stats', 'METHOD': 'GET'}
REGIONAL_RESPONSE_TIME = {'ENDPOINT': '/regional_response_time', 'METHOD': 'GET'}
AGENT_SUMMARY = {'ENDPOINT': '/agent_summary', 'METHOD': 'GET'}
AGENTS = {'ENDPOINT': '/agents', 'METHOD': 'GET'}
HOURLY_STATS = {'ENDPOINT': '/hourly_stats', 'METHOD': 'GET'}
AGENT_REPORT = {'ENDPOINT': '/agent_report', 'METHOD': 'GET'}
DASHBOARD_15_STATS = {'ENDPOINT': '/dashboard15stats', 'METHOD': ['GET', 'POST']}
CATEGORY_WISE_RESPONSE = {'ENDPOINT': '/get_categorywise_response', 'METHOD': 'GET'}
DISTRICT_CATEGORICAL_RESPONSE = {'ENDPOINT': '/district_catgorical_response', 'METHOD': 'GET'}
ALERTS = {'ENDPOINT': '/get_alerts', 'METHOD': 'GET'}
ALERT_DETAILS = {'ENDPOINT': '/get_alert_details', 'METHOD': 'GET'}
GET_RESPONSE_REPORT = {'ENDPOINT': '/get_response_report', 'METHOD': 'GET'}
NEGATIVE_CALLS = {'ENDPOINT': '/get_negative_calls', 'METHOD': 'GET'}
PUCAR_DASHBOARD_DATA = {'ENDPOINT': '/get_pucar_data', 'METHOD': 'GET'}
VEHICLE_STATS = {'ENDPOINT': '/get_vehicle_stats', 'METHOD': 'GET'}
GENERATE_DIST_REPORT = {'ENDPOINT': '/generate_district_report', 'METHOD': 'GET'}
DETAILED_DISTRICT_RESPONSE = {'ENDPOINT': '/detailed_district_report', 'METHOD': 'GET'}
CONF_CALLS_REPORT = {'ENDPOINT': '/conf_calls_report', 'METHOD': 'GET'}

"""ALL DISTRICTS FROM MASTER DATABASE FOR USE AS DICTIONARY MAPPING"""
DISTRICTS_MAPPING = [(1, "Sheikhupura"), (2, "Nankana Sb"), (3, "Kasur"), (4, "Gujranwala"), (6, "Hafizabad"),
                     (7, "Gujrat"), (8, "M.B. Din"), (9, "Sialkot"), (10, "Narowal"), (11, "Rawalpindi"),
                     (13, "Attock"), (14, "Jhelum"), (15, "Chakwal"), (16, "Sargodha"), (17, "Khushab"),
                     (18, "Mianwali"), (19, "Bhakkar"), (20, "Faisalabad"), (22, "Jhang"), (23, "Chiniot"),
                     (24, "T.T. Singh"), (25, "Multan"), (27, "Lodhran"), (28, "Khanewal"), (29, "Vehari"),
                     (30, "Sahiwal"), (31, "Okara"), (32, "Pakpattan"), (33, "D.G. Khan"), (34, "Rajanpur"),
                     (35, "Muzaffargarh"), (36, "Layyah"), (37, "Bahawalpur"), (38, "Bahawalnagar"),
                     (39, "Rahimyar Khan"), (40, "Lahore")]

# , (41, "Lahore-Test"), (42, "Female-15"), (43, "Kot Addu"),(44, "Wazirabad"), (45, "Potohari-15"), (46, "Murree")

DISTRICTS_DICTIONARY = {id: name for id, name in DISTRICTS_MAPPING}
REVERSED_DISTRICTS_DICTIONARY = {name: id for id, name in DISTRICTS_MAPPING}

"""DEFINED CUTOFFS FOR CONTROLLED RESPONSE TIME"""
FIR_RATE = 0.2950

"""NUMBER OF DAYS FOR DATE CHANGING TO PROCESS STATS"""
DELTA_DAYS = 1

"""PPIC3 SHIFTS"""
SHIFTS = {
    'A': ('06', '13'),
    'B': ('14', '21'),
    'C': ('22', '05')}

"""POLICE SHIFTS"""
DAY_SHIFT = '8am-8pm'
NIGHT_SHIFT = '8pm-8am'

"""REGIONS FOR POLICE STATION SEGREGATION"""
ALL_REGIONS = ["rural", "urban"]

""""VEHICLE SESSION CONFIGURATION"""
SESSION_FILE = '../DataScrappingSessions/session.pkl'
SESSION_VEHICLE_FILE = "../DataScrappingSessions/vehicle_session.pkl"
SESSION_EXPIRY_TIME = timedelta(minutes=50)
SESSION_CHECK_INTERVAL = timedelta(minutes=320)

"""URLS FOR DASHBOARDS"""
VEHICLE_LOGIN_URL = 'https://police15.psca.gop.pk/public/login'
VEHICLE_STATS_URL = 'https://police15.psca.gop.pk/public/punjab_vehicle'
LIVE_VEHICLE_DATA = 'https://police15.psca.gop.pk/public/field_live_location'

"""PUCAR 15 DASHBOARD CREDENTIALS"""
V_CREDENTIALS = {
    'username': os.getenv('V_USER_NAME'),
    'password': os.getenv('V_PASSWORD'),
    '_token': ''
}

AP_CREDENTIALS = {
    'username': os.getenv('AP_USER_NAME'),
    'password': os.getenv('AP_PASSWORD'),
    '_token': ''
}

""""SESSION HEADER"""
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

"""DATE_FORMATS"""
YM_DATE = '%Y-%m-%d'
YMD_TIME = '%Y-%m-%d 00:00:00'
YMD_HMS = '%Y-%m-%d %H:%M:%S'

"""INTERNAL SERVER ERROR"""
INTERNAL_ERROR_MESSAGE = {"error": "Internal Server Error"}
NO_DATA_ERROR_MESSAGE = {'message': 'No Data Found'}

"""ERROR CODES"""
BAD_REQUEST_ERROR = 400
OK_REQUEST = 200
UNAUTHORIZED_REQUEST_ERROR = 401
INTERNAL_SERVER_ERROR = 500

"""Skills for Admin Dashboard"""

SKILLS = ['Attock-15', 'Bahawalnagar-15', 'Bahawalpur-15', 'Bhakkar-15', 'Chakwal-15',
          'Chiniot-15', 'D-G-Khan-15', 'english-15', 'Faisalabad-15', 'Gujranwala', 'Gujrat-15',
          'Hafizabad-15', 'Jhang-15', 'Jhelum-15', 'kasur-15', 'Khanewal-15', 'Khushab-15', 'Lahore-15',
          'Layyah-15', 'Lodhran-15', 'M-B-Din-15', 'Mianwali-15', 'minorities-15', 'multan-15', 'Muzaffargarh-15',
          'nankana-sb-15', 'Narowal-15', 'Okara-15', 'Pakpattan-15', 'potohari-15', 'punjabi-15',
          'Rahimyar-Khan-15', 'Rajanpur-15', 'Rawalpindi-15', 'Sahiwal-15', 'Sargodha-15', 'sheikhupura-15',
          'Sialkot-15', 'siraiki-15', 'T-T-Singh-15', 'Vehari-15']

"""Crime Cateogires for FIR"""
CAW = ['Rape', 'Physical Threats / Harrasment', 'Sexual Assault/ Harrasment To Women',
       'Prostitution/ Brothel House', 'Domestic Violence', 'Female Kidnapping/ Abduction']

CACH = ['Child Kidnapping', 'Child Abuse / Molestation']

CAP = ['Kidnapping for Ransom', 'Attempt to Kidnap / Abduct', 'Assault on Govt. Officials',
       'Murder', 'Street Fight', 'Hurt / Injuries', 'Criminal Intimidation (Threat with Weapon)',
       'Male Kidnapping/ Abduction', 'Attempt to Murder', 'Other Assault']


"""MODULARITY OF API QUERIES"""
# common columns that are frequently used
PROCESSED_COLUMNS = [
    "total_calls", "siraiki", "punjabi", "potohari", "english", "traffic", "vwps",
    "app_alerts", "transfered", "call_backs", "video_calls", "estimated_response_time",
    "succ_conf_calls","unsucc_conf_calls", "vccs", "vcm", "generated_cases"
]

# Base query for processed_data table
BASE_PROCESSED_DATA_QUERY = """
SELECT {columns}
FROM processed_data
WHERE district_id NOT IN ('0','41','42','43','44','45','46')
AND district_id IS NOT NULL
{additional_conditions}
"""

PROCESSED_COLUMNS_WITH_DISTRICT_ID = ["district_id"] + PROCESSED_COLUMNS
PROCESSED_COLUMNS_WITH_DATE = ["date"] + PROCESSED_COLUMNS


#common additional conditions
DATE_CONDITION = " date = ? "
DATE_RANGE_CONDITION = " AND date BETWEEN ? AND ? "
DATE_RANGE_EXTENDED_CONDITION = " AND (((date = ?) AND (hour BETWEEN '20' AND '23')) OR ((date = ?) AND (hour BETWEEN '00' AND '07'))) "
DISTRICT_CONDITION = " AND district_id = ? "
AGENT_CONDITION = " AND agent = ? "
HOUR_RANGE_CONDITION = " AND hour BETWEEN '08' AND '19' "
UNIX_DATETIME_CONDITION = " AND datetime(time_id, 'unixepoch','localtime') BETWEEN ? AND ?"
UNIX_DATE_CONDITION = " AND DATE(datetime(time_id, 'unixepoch','localtime')) BETWEEN ? AND ?"


"""REGIONAL_RESPONSE_TIME_AVG QUERY COMPONENTS FOR DASHBOARD AND REPORTS"""
# Base query components
REGIONAL_RESPONSE_TIME_AVG_BASE_QUERY = """
    SELECT {columns}
    FROM response_time
    WHERE {conditions}
"""

# Common conditions
REGIONAL_RESPONSE_COMMON_CONDITIONS = [
    "region_category IS NOT NULL",
    "district_id NOT IN ('0','41','42','43','44','45','46')",
    "district_id IS NOT NULL",
    "parent_id = 0",
    "response_time is NOT NULL AND response_time > 0"
]

# Column templates
REGIONAL_RESPONSE_TIME_COLUMNS = {
    "basic": ["region_category", "AVG(response_time) AS avg_response_time"],
    "with_district": ["district_id", "region_category", "AVG(response_time) AS avg_response_time"],
    "with_date": ["date", "region_category", "AVG(response_time) AS avg_response_time"]
}

# Group by templates
REGIONAL_RESPONSE_TIME_GROUP_BY = {
    "basic": ["region_category"],
    "with_district": ["district_id", "region_category"],
    "with_date": ["date", "region_category"]
}

"""AGENT STATS QUERY COMPONENTS"""

AGENT_STATS_BASE_QUERY = """
        SELECT 
            agent_name,
            {column_str}
        FROM 
            agent_stats
        {condition_str}
        {group_by_str}
        {order_by_str}
"""


AGENT_STATS_COMMON_COLUMNS = {
    "hoax_calls": "sum(hoax_calls) as hoax_calls",
    "consult_calls": "sum(consult_calls) as consult_calls",
    "repeated_calls": "sum(repeated_calls) as repeated_calls",
    "avg_wrapup_time": "avg(avg_wrapup_time) as avg_wrapup_time",
    "generated_cases": "sum(generated_cases) as generated_cases",
    "positive_feedback": "sum(positive_feedback) as positive_feedback",
    "negative_feedback": "sum(negative_feedback) as negative_feedback"
}

AGENT_STATS_CONDITIONS = {
    "date_range": DATE_RANGE_CONDITION[4:],
    "agent": AGENT_CONDITION[4:],
    "agent_null": " agent_name IS NOT NULL",
}

"""AVG RESPONSE TIME QUERY COMPONENTS"""
RESPONSE_TIME_BASE_QUERY = """
SELECT {columns}
FROM response_time
WHERE {conditions}
{group_by}
"""

RESPONSE_TIME_COLUMNS = {
    "basic": ["district_id", "AVG(response_time) AS avg_response_time"],
    "with_date": ["date", "AVG(response_time) AS avg_response_time"],
    "avg_only": ["AVG(response_time) AS avg_response_time"]
}

RESPONSE_TIME_GROUP_BY = {
    "basic": "GROUP BY district_id",
    "with_date": "GROUP BY date"
}

RESPONSE_TIME_COMMON_CONDITIONS = [
    "district_id NOT IN ('0','41','42','43','44','45','46')",
    "district_id IS NOT NULL",
    "response_time is NOT NULL AND response_time > 0",
    "parent_id = 0"
]

"""RESPONSE TIME STATS QUERY COMPONENTS"""
RESPONSE_TIME_STATS_QUERY = """
                        SELECT 
                            district_id,
                            {time_range_counts}
                        FROM
                            response_time
                        WHERE 
                            {conditions}
                        {group_by}"""

RESPONSE_TIME_STATS_RANGES = [
    (60, "less_than_60"),
    (300, "between_60_300"),
    (600, "between_300_600"),
    (1800, "between_600_1800"),
    (3600, "between_1800_3600"),
    (float('inf'), "above_3600")
]

""" PUNJAB EMERGENCY-i APIs ENDPOINTS AND METHODS"""
LOGIN = {'ENDPOINT': '/login', 'METHOD': 'POST'}
DASHBOARD_PUNJAB = {'ENDPOINT': '/dashboard_punjab', 'METHOD': 'POST'}
PUNJAB_MORE_INFO = {'ENDPOINT': '/punjab_more_info', 'METHOD': 'POST'}
DISTRICTWISE_STATS = {'ENDPOINT': '/districtwise_counts', 'METHOD': 'POST'}
DISTRICTWISE_MORE_INFO = {'ENDPOINT': '/districtwise_more_info', 'METHOD': 'POST'}
PUNJABTODAY_CASE_DETAILS = {'ENDPOINT': '/case_details', 'METHOD': 'POST'}
DIST_CATEGORY_DETAILS = {'ENDPOINT': '/district_categorywise_details', 'METHOD': 'POST'}
PSWISE_CATEGORIES = {'ENDPOINT': '/pswise_categories', 'METHOD': 'POST'}
PREDICTIVE_FORECAST = {'ENDPOINT': '/predicitve_forecast', 'METHOD': 'POST'}
DATEWISE_FORECAST = {'ENDPOINT': '/datewise_forecast', 'METHOD': 'POST'}
EMERGENCY_15_INTEGRATION = {'ENDPOINT': '/emergency15_integration', 'METHOD': 'POST'}
ADD_REMARKS = {'ENDPOINT': '/add_remarks', 'METHOD': 'POST'}
UPDATE_REMARKS = {'ENDPOINT': '/update_remarks', 'METHOD': 'POST'}
GET_REMARKS = {'ENDPOINT': '/get_remarks', 'METHOD': 'POST'}
DASHBOARDS_MORE_INFO = {'ENDPOINT': '/dashboards_details', 'METHOD': 'POST'}
CM_DIST_RESPONSE_TIME = {'ENDPOINT': '/cm_dist_response_time', 'METHOD': 'POST'}
CM_PS_RESPONSE_TIME = {'ENDPOINT': '/cm_ps_response_time', 'METHOD': 'POST'}
CONFERENCE_CALL_STATS = {'ENDPOINT': '/conferencecall_stats', 'METHOD': 'POST'}
DISTRICT_FIR_DATA = {'ENDPOINT': '/district_fir_stats', 'METHOD': 'POST'}
PS_FIR_DATA = {'ENDPOINT': '/ps_fir_stats', 'METHOD': 'POST'}
ALERT_RESPONSETIME = {'ENDPOINT': '/response_time_alerts', 'METHOD': 'POST'}
VEHICLE_LOCATIONS = {'ENDPOINT': '/vehicle_locations', 'METHOD': 'POST'}
VWPS_STATS = {'ENDPOINT': '/vwps_stats', 'METHOD': 'POST'}
VCCS_STATS = {'ENDPOINT': '/vccs_stats', 'METHOD': 'POST'}
VCM_STATS = {'ENDPOINT': '/vcm_stats', 'METHOD': 'POST'}
CRIME_TRENDS = {'ENDPOINT': '/crime_trends', 'METHOD': 'POST'}
CALLER_FEEDBACK = {'ENDPOINT': '/caller_feedback', 'METHOD': 'POST'}
BLOOD_DONATION = {'ENDPOINT': '/blood_donation', 'METHOD': 'POST'}
ESCALATED_CASES = {'ENDPOINT': '/escalated_cases', 'METHOD': 'POST'}
CRIME_TREND_CASES = {'ENDPOINT': '/crime_trend_cases', 'METHOD': 'POST'}
NEGATIVE_FEEDBACK_CASES = {'ENDPOINT': '/negative_feedback_cases', 'METHOD': 'POST'}
ADD_MESSAGE = {'ENDPOINT': '/add_message', 'METHOD': 'POST'}
CHAT_HISTORY = {'ENDPOINT': '/chat_history', 'METHOD': 'POST'}
SUBSCRIBE_TOPIC = {'ENDPOINT': '/subscribe_user_topic', 'METHOD': 'POST'}
UNSUBSCRIBE_TOPIC = {'ENDPOINT': '/unsubscribe_user_topic', 'METHOD': 'POST'}
GET_NOTIFICATIONS = {'ENDPOINT': '/get_notifications', 'METHOD': 'POST'}
UPDATE_PASSWORD = {'ENDPOINT': '/update_password', 'METHOD': 'PUT'}
CRIME_REOCCURENCE_CASE = {'ENDPOINT': '/crime_reoccurence_case', 'METHOD': 'POST'}
PS_CONFERENCE_CALL_STATS = {'ENDPOINT': '/ps_conferencecall_stats', 'METHOD': 'POST'}
CALLER_FEEBACK_PSWISE = {'ENDPOINT': '/caller_feedback_pswise', 'METHOD': 'POST'}
USER_ANALYTICS = {'ENDPOINT': '/user_analytics', 'METHOD': 'POST'}


PS_CASE_DETAILS = {'ENDPOINT': '/ps_case_details', 'METHOD': 'POST'} # currently unused API

"""CATEGORIES MAPPING"""
CATEGORIES = {
    'murder': ['Murder', 'Attempt to Murder'],  # level3
    'dacoity_with_murder': ['Dacoity with Murder'],
    'dacoity': ['Highway/Road/Street Dacoity', 'House Dacoity', 'Any Other Dacoity', 'Shop Dacoity', 'Cattle Dacoity',
                'Patrol Pump Dacoity', 'Jewellery Shop Dacoity'],
    # level3
    'attempt_to_murder': [],  # level 3
    'aerial_firing': ['Aerial Firing'],  # level3
    'rape': ["Sexual Assault/ Harrasment To Women"],  # level 3
    'kidnapping': ["Child Kidnapping", "Female Kidnapping/ Abduction", "Male Kidnapping/ Abduction",
                   "Kidnapping for Ransom",
                   "Attempt to Kidnap / Abduct", "Child Kidnapping "],  # level3
    'hurt': ["Hurt / Injuries", "Street Fight", "Other Assault", "Criminal Intimidation (Threat with Weapon)",
             "Assault on Govt. Officials", "Acid Throwing", "Other Help"],
    # level3
    'robbery_snatching': ['Highway/Road/Street Robbery', 'Any Other Robbery', 'Cattle Robbery', 'House Robbery',
                          'Shop Robbery',
                          'Patrol Pump Robbery', 'Bank/Money Exchange/ ATM Robbery', 'Car Snatching',
                          'Other Vehicles Snatching',
                          'Snatching/Jhapatta', 'Motorcycle Snatching', 'Jewellery Shop Robbery',
                          'Robbery with Murder'],
    # level3
    'motorcycle_theft': ['Motorcycle Theft'],  # level3
    'car_theft': ['Car Theft'],  # level3
    'theft': ['Mobile Theft', 'Any Other Theft', 'Cattle theft', 'Transformer/ Motor Theft', 'Pick Pocketing',
              'Purse / Wallet / Luggage Theft',
              'Cycle Theft', 'Weapon Theft', 'Other Vehicles Theft', 'House Burglary', 'Shop Burglary',
              'Other Burglary'],
    # level3
    'burglary': ['House Burglary', 'Shop Burglary', 'Other Burglary'],  # level3
    'terrorist_act': ['Firing on Police', 'Suicidal Attack/ Bomb Blast/ Terrorist Attack'],  # level3
    'other_person': ['Prostitution/ Brothel House', 'Acid Throwing', 'Hurt / Injuries', 'Street Fight', 'Other Assault',
                     'Physical Threats / Harrasment', 'Domestic Violence',
                     'Criminal Intimidation (Threat with Weapon)'],
    'other_property': ['Attempt to Illegal Possession of Land/ Premises'],  # level3
    'child_abuse': ['Rape', 'Child Abuse / Molestation']  # level3
}

REGIONAL_CATEGORY_RT_QUERY = """        
               WITH category_region AS (
                    SELECT DISTINCT categories.category, regions.region_category
                    FROM (
                        SELECT 'kidnapping' AS category UNION
                        SELECT 'robbery_snatching' UNION
                        SELECT 'theft' UNION
                        SELECT 'minorities' UNION
                        SELECT 'dacoity' UNION
                        SELECT 'dacoity_with_murder' UNION
                        SELECT 'rape' UNION
                        SELECT 'terrorist_act' UNION
                        SELECT 'child_abuse' UNION
                        SELECT 'murder' UNION
                        SELECT 'car_theft' UNION
                        SELECT 'aerial_firing' UNION
                        SELECT 'motorcycle_theft' UNION
                        SELECT 'burglary' UNION
                        SELECT 'other_person' UNION
                        SELECT 'other_property'
                    ) AS categories
                    CROSS JOIN (SELECT DISTINCT region_category FROM response_time WHERE region_category IS NOT NULL) AS regions
                )
                
                SELECT 
                    cr.category,
                    cr.region_category,
                    COALESCE(AVG(rt.response_time), 0) AS avg_response_time
                FROM 
                    category_region cr
                LEFT JOIN (
                    SELECT 
                        CASE 
                            WHEN level3_case_nature IN ('Male Kidnapping/ Abduction', 'Female Kidnapping/ Abduction', 
                                                      'Child Kidnapping', 'Attempt to Kidnap / Abduct', 
                                                      'Kidnapping for Ransom', 'Child Kidnapping ') 
                            THEN 'kidnapping'
                            WHEN level3_case_nature IN ('Highway/Road/Street Robbery','Any Other Robbery', 'Cattle Robbery',
                                                        'House Robbery', 'Shop Robbery','Patrol Pump Robbery','Bank/Money Exchange/ ATM Robbery','Car Snatching',
                                                        'Other Vehicles Snatching','Snatching/Jhapatta','Motorcycle Snatching','Jewellery Shop Robbery')
                            THEN 'robbery_snatching'
                            WHEN level3_case_nature IN ('Mobile Theft','Any Other Theft','Cattle theft',
                                                        'Transformer/ Motor Theft','Pick Pocketing','Purse / Wallet / Luggage Theft',
                                                        'Cycle Theft','Weapon Theft')
                            THEN 'theft'
                            WHEN queue = 'minorities-15' THEN 'minorities'
                            WHEN level3_case_nature IN ('Highway/Road/Street Dacoity','House Dacoity' ,'Any Other Dacoity' , 'Shop Dacoity' ,'Cattle Dacoity',
                                                        'Patrol Pump Dacoity','Jewellery Shop Dacoity') THEN 'dacoity'
                            WHEN level3_case_nature IN ('Dacoity with Murder') THEN 'dacoity_with_murder'
                            WHEN level3_case_nature IN ('Rape','Sexual Assault/ Harrasment To Women') THEN 'rape'
                            WHEN level3_case_nature IN ('Firing on Police','Suicidal Attack/ Bomb Blast/ Terrorist Attack') THEN 'terrorist_act'
                            WHEN level3_case_nature IN ('Child Abuse / Molestation') THEN 'child_abuse'
                            WHEN level3_case_nature IN ('Murder','Attempt to Murder') THEN 'murder'
                            WHEN level3_case_nature IN ('Car Theft') THEN 'car_theft'
                            WHEN level3_case_nature = 'Aerial Firing' THEN 'aerial_firing'
                            WHEN level3_case_nature = 'Motorcycle Theft' THEN 'motorcycle_theft'
                            WHEN level3_case_nature IN ('House Burglary','Shop Burglary','Other Burglary','Bank Burglary') THEN 'burglary'
                            WHEN level3_case_nature IN ('Prostitution/ Brothel House','Acid Throwing','Hurt / Injuries','Street Fight','Other Assault',
                                                        'Physical Threats / Harrasment','Domestic Violence','Criminal Intimidation (Threat with Weapon)')
                                                        THEN 'other_person'
                            WHEN level3_case_nature IN ('Attempt to Illegal Possession of Land/ Premises') THEN 'other_property'
                        END as category,
                        region_category,
                        response_time
                    FROM 
                        response_time 
                    WHERE 
                        (
                        level3_case_nature IN (
                            'Murder','Robbery with Murder','Dacoity with Murder', 'Attempt to Murder', 'Aerial Firing','Child Abuse / Molestation', 'Motorcycle Theft',
                            'Male Kidnapping/ Abduction', 'Female Kidnapping/ Abduction', 'Child Kidnapping', 'Attempt to Kidnap / Abduct', 'Kidnapping for Ransom', 'Child Kidnapping ',
                            'Rape','Sexual Assault/ Harrasment To Women','Highway/Road/Street Robbery','Any Other Robbery', 'Cattle Robbery','House Robbery', 'Shop Robbery',
                            'Patrol Pump Robbery','Bank/Money Exchange/ ATM Robbery','Car Snatching','Other Vehicles Snatching','Snatching/Jhapatta','Motorcycle Snatching','Jewellery Shop Robbery',
                            'Highway/Road/Street Dacoity','House Dacoity' ,'Any Other Dacoity' , 'Shop Dacoity' ,'Cattle Dacoity','Patrol Pump Dacoity','Jewellery Shop Dacoity','Car Theft',
                            'Mobile Theft','Any Other Theft','Cattle theft','Transformer/ Motor Theft','Pick Pocketing','Purse / Wallet / Luggage Theft','Cycle Theft','Weapon Theft',
                            'Firing on Police','Suicidal Attack/ Bomb Blast/ Terrorist Attack','House Burglary','Shop Burglary','Other Burglary','Bank Burglary',
                            'Prostitution/ Brothel House','Acid Throwing','Hurt / Injuries','Street Fight','Other Assault','Physical Threats / Harrasment',
                            'Domestic Violence','Criminal Intimidation (Threat with Weapon)','Attempt to Illegal Possession of Land/ Premises'
                        )
						OR queue = 'minorities-15'
						)
                        AND date BETWEEN ? AND ?
                        AND parent_id = 0
                        AND response_time IS NOT NULL 
                        AND response_time > 0
                        AND region_category IS NOT NULL
                ) AS rt
                ON cr.category = rt.category
                AND cr.region_category = rt.region_category
                GROUP BY 
                    cr.category, cr.region_category;
"""

FIR_QUERY = """
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
            lead_id
        FROM response_time
        WHERE date BETWEEN ? AND ?
    )
    SELECT 
        c.category,
        COUNT(DISTINCT f.lead_id) as fir_count
    FROM categorized_cases c
    LEFT JOIN fir_db.leads_in_fir f ON c.lead_id = f.lead_id 
    WHERE c.category IS NOT NULL
    GROUP BY c.category
"""

COMB_DASHBOARD_COND = {
    'Recieved_vccs': "",
    'Under_inquiry_vccs': " final_status_id IN (8,12) AND handed_over_to IN (1, 3, 4, 5)",
    'Escalated_vccs': " final_status_id = 7",
    'Fir_vccs': " is_fir_registered = 1 OR is_challan_submitted = 1",
    'Challan_vccs': "  is_challan_submitted = 1",
    'Resolved_vccs': " (final_status_id = 6 OR (final_status_id = 12 AND handed_over_to = 2 ))",
    'Recieved_vwps': "",
    'Under_inquiry_vwps': " final_status_id in (1,7,8)",
    'Escalated_vwps': " final_status_id = 7",
    'Fir_vwps': " final_status_id in (2,3,5,9,10,11)",
    'Challan_vwps': " final_status_id in (5,9,10,11)",
    'Recieved_vcm': "",
    'Under_inquiry_vcm': " final_status_id in (1,8)",
    'Escalated_vcm': " final_status_id = 7",
    'Fir_vcm': " final_status_id in (2,3,5,9,10,11)",
    'Challan_vcm': " final_status_id in (5,9,10,11)",
    'Resolved_vcm': " final_status_id = 6"
}

FIR_API_URL = "https://police15.psca.gop.pk/public/fir/police-stations"

RANKS_USERNAME = {
    "ig.punjab": "IG Punjab",
    "addligp.sppo@punjabpolice.gov.pk": "Addl: IGP South Punjab",
    "ccpo.lhr@punjabpolice.gov.pk": "CCPO Lahore",
    "digops.lhr@punjabpolice.gov.pk": "DIG Operations Lahore",
    "diginv.lhr@punjabpolice.gov.pk": "DIG Investigation Lahore",
    "digsecurity@punjabpolice.gov.pk": "DIG Security Division Lahore",
    "rpo.bwp@punjabpolice.gov.pk": "Regional Police Officer Bahawalpur",
    "rpo.fsd@punjabpolice.gov.pk": "Regional Police Officer Faisalabad",
    "rpo.grw@punjabpolice.gov.pk": "Regional Police Officer Gujranwala",
    "rpo.skp@punjabpolice.gov.pk": "Regional Police Officer Sheikhupura",
    "rposargodha@punjabpolice.gov.pk": "Regional Police Officer Sargodha",
    "rpo.swl@punjabpolice.gov.pk": "Regional Police Officer Sahiwal",
    "rpo.dgk@punjabpolice.gov.pk": "Regional Police Officer D.G.Khan",
    "rpo.rwp@punjabpolice.gov.pk": "Regional Police Officer Rawalpindi",
    "rpo.mux@punjabpolice.gov.pk": "Regional Police Officer Multan",
    "cpo.fsd@punjabpolice.gov.pk": "City Police Officer Faisalabad",
    "cpo.grw@punjabpolice.gov.pk": "City Police Officer Gujranwala",
    "cpo.mux@punjabpolice.gov.pk": "City Police Officer Multan",
    "cpo.rwp@punjabpolice.gov.pk": "City Police Officer Rawalpindi",
    "dpo.att@punjabpolice.gov.pk": "District Police Officer Attock",
    "dpo.bwn@punjabpolice.gov.pk": "District Police Officer Bahawal Nagar",
    "dpo.bwp@punjabpolice.gov.pk": "District Police Officer Bahawalpur",
    "dpo.bkk@punjabpolice.gov.pk": "District Police Officer Bhakkar",
    "dpo.ckl@punjabpolice.gov.pk": "District Police Officer Chakwal",
    "dpo.cot@punjabpolice.gov.pk": "District Police Officer Chiniot",
    "dpo.dgk@punjabpolice.gov.pk": "District Police Officer DG Khan",
    "dpo.grt@punjabpolice.gov.pk": "District Police Officer Gujrat",
    "dpo.hfz@punjabpolice.gov.pk": "District Police Officer Hafizabad",
    "dpo.jhg@punjabpolice.gov.pk": "District Police Officer Jhang",
    "dpo.jm@punjabpolice.gov.pk": "District Police Officer Jhelum",
    "dpo.ksr@punjabpolice.gov.pk": "District Police Officer Kasur",
    "dpo.knw@punjabpolice.gov.pk": "District Police Officer Khanewal",
    "dpo.ksb@punjabpolice.gov.pk": "District Police Officer Khushab",
    "dpo.lya@punjabpolice.gov.pk": "District Police Officer Layyah",
    "dpo.ldh@punjabpolice.gov.pk": "District Police Officer Lodhran",
    "dpo.mbd@punjabpolice.gov.pk": "District Police Officer MB Din",
    "dpo.mwi@punjabpolice.gov.pk": "District Police Officer Mianwali",
    "dpo.mzg@punjabpolice.gov.pk": "District Police Officer Muzafargarh",
    "dpo.nks@punjabpolice.gov.pk": "District Police Officer Nankana",
    "dpo.nrl@punjabpolice.gov.pk": "District Police Officer Narowal",
    "dpo.oka@punjabpolice.gov.pk": "District Police Officer Okara",
    "dpo.pp@punjabpolice.gov.pk": "District Police Officer Pak Patan",
    "dpo.ryk@punjabpolice.gov.pk": "District Police Officer R.Y.Khan",
    "dpo.rjr@punjabpolice.gov.pk": "District Police Officer Rajanpur",
    "dpo.swl@punjabpolice.gov.pk": "District Police Officer Sahiwal",
    "dpo.sgd@punjabpolice.gov.pk": "District Police Officer Sargodha",
    "dpo.skp@punjabpolice.gov.pk": "District Police Officer Sheikhupura",
    "dpo.skt@punjabpolice.gov.pk": "District Police Officer Sialkot",
    "dpo.tts@punjabpolice.gov.pk": "District Police Officer T.T.Singh",
    "dpo.vri@punjabpolice.gov.pk": "District Police Officer Vehari",
    "sdpo.baghbanpura@punjabpolice.gov.pk": "sdpo Baghbanpura",
    "sho.baghbanpura@punjabpolice.gov.pk": "sho baghbanpura",
    "dpo.modeltown@punjabpolice.gov.pk": "dpo Model town"
}

CRIME_TRENDS_CATEGORY = {
    'total': """(SUM(dacoity) + SUM(burglary) + SUM(robbery_snatching) + SUM(motorcycle_theft) + SUM(car_theft) + SUM(vehicle_theft) + 
                   SUM(vehicle_snatching) + SUM(car_snatching) + SUM(motorcycle_snatching))""",
    'vehicle_snatching': "(SUM(vehicle_snatching) + SUM(car_snatching) + SUM(motorcycle_snatching))",
    'car_snatching': "(SUM(car_snatching))",
    'motorcycle_snatching': "(SUM(motorcycle_snatching))",
    'dacoity': "(SUM(dacoity))",
    'burglary': "(SUM(burglary))",
    'robbery_snatching': "(SUM(robbery_snatching))",
    'motorcycle_theft': "(SUM(motorcycle_theft))",
    'car_theft': "(SUM(car_theft))",
    'vehicle_theft': "(SUM(motorcycle_theft) + SUM(car_theft) + SUM(vehicle_theft))"
}

MULTAN_DIVISON_MAPPING = {
    'Multan Cantt': 'Cantt Division',
    'Muzaffarabad': 'Cantt Division',
    'Mumtazabad': 'Cantt Division',
    'Gulgasht': 'Gulgasht Division',
    'Sadar': 'Gulgasht Division',
    'New Multan': 'City Division',
    'Haram Gate': 'City Division',
    'Dehli Gate': 'City Division',
    'Makhdoom Rashid': 'Saddar Division',
    'Shujahbad': 'Saddar Division',
    'Jalalpur Pirwala': 'Saddar Division'
}

LAHORE_DIVISION_MAPPING = {
    'Badami Bagh': 'City Division',
    'Baghbanpura.': 'Cantt Division',
    'Shahdara': 'City Division',
    'Model Town': 'Model Town Division',
    'Defence': 'Cantt Division',
    'Iqbal Town': 'Iqbal Town Division',
    'Harbanspura': 'Cantt Division',
    'Garden Town': 'Model Town Division',
    'Raiwind': 'Sadar Division',
    'Town Ship': 'Sadar Division',
    'Kahna': 'Model Town Division',
    'Gulberg': 'Model Town Division',
    'Mughalpura': 'Civiline Division',
    'Sabzazar': 'Sadar Division',
    'Misri Shah': 'Civiline Division',
    'Nawankot': 'Iqbal Town Division',
    'Chung': 'Sadar Division',
    'North Cantt.': 'Cantt Division',
    'Shafiqabad': 'City Division',
    'Qila Gujar Sinch': 'Civiline Division',
    'Samanabad': 'Civiline Division',
    'Lower Mall': 'City Division',
    'Cantt.': 'Cantt Division',
    'Muslim Town': 'Iqbal Town Division',
    'Manawan': 'Cantt Division',
    'Ichhra': 'Model Town Division',
    'Islampura': 'City Division',
    'Old Anarkali': 'Civiline Division',
    'Naulakha': 'City Division',
    'Burki': 'Cantt Division',
    'Gulshan Ravi': 'Iqbal Town Division',
    'Gowalmandi': 'City Division',
    'Rang Mehal': 'City Division',
    'Tibbi City': 'City Division',
    'Race Course': 'Civiline Division',
    'Women Race Course' : 'Civiline Division'

}

RAWALPINDI_DIVISION_MAPPING = {
    'Civil Lines': 'Potohar Division',
    'Rawalpindi Cantt': 'Potohar Division',
    'Taxila': 'Potohar Division',
    'Waris Khan': 'Rawal Division',
    'City': 'Rawal Division',
    'New Town': 'Rawal Division',
    'Sadar': 'Saddar Division',
    'Gujjar Khan': 'Saddar Division',
    'Kahuta': 'Saddar Division',
}

GUJRANWALA_DIVISION_MAPPING = {
    'Cantt': 'Civil Line Division',
    'Kamoke': 'Sadar Division',
    'Noushera Virka': 'Sadar Division',
    'Wazirabad': 'Wazirabad Division',
    'Model Town' : 'City Division',
    'Kotwali' : 'City Division',
    'Khiali' : 'City Division',
    'Qila Dedar Singh' : 'City Division',
    'Satellite Town' : 'Civil Line Division',
    'Peoples Colony' : 'Civil Line Division'
}

FAISALABAD_DIVISION_MAPPING = {
    'Jarranwala' : 'Jarranwala Division',
    'Gulberg' : 'Lyallpur Division',
    'Sargodha Road' : 'Madina Town Division',
    'Tandlianwala' : 'Sadar Division',
    'Batala Colony' : 'Iqbal Town Division' ,
    'Sadar F/abad' : 'Iqbal Town Division',
    'Kotwali' : 'Lyallpur Division',
    'Nishatabad' : 'Madina Town Division',
    'Factory Area' : 'Iqbal Town Division',
    'Civil Lines' : 'Lyallpur Division',
    'People Colony' : 'Madina Town Division',
    'Khurrianwala' : 'Jarranwala Division',
    'Sammundri' : 'Sadar Division' ,
}

# Define API endpoints
FEEDBACK_15_STATS_URL = "https://police15.psca.gop.pk/public/caller/feedback/get_status_count"
FEEDBACK_DISTRICT_COUNT_URL = "https://police15.psca.gop.pk/public/caller/feedback/get_dist_status_count"

CRIME_TRENDS_CATEGORIES = {
    "motorcycle_theft": "level3_case_nature = 'Motorcycle Theft'",
    "murder": "level2_case_nature = 'Murder'",
    "sexual_assault": "level2_case_nature = 'Sexual Assault'",
    "firing": "level3_case_nature = 'Aerial Firing'",
    "kidnapping": "level2_case_nature = 'Kiddnapping / Abduction'",
    "dacoity": "level2_case_nature IN ('Dacoity')",
    "robbery_snatching": "level2_case_nature IN ('Robbery/Snatching')",
    "vehicle_theft": "level3_case_nature IN ('Cycle Theft', 'Other Vehicles Theft')",
    "burglary": "level2_case_nature IN ('Burglary')",
    "car_theft": "level3_case_nature = 'Car Theft'",
    "motorcycle_snatching": "level3_case_nature = 'Motorcycle Snatching'",
    "car_snatching": "level3_case_nature = 'Car Snatching'",
    "vehicle_snatching": "level2_case_nature IN ('Vehicle Snatching')",
    "crime_against_property": """
        (level2_case_nature IN ('Vehicle Snatching', 'Burglary', 'Robbery/Snatching', 'Dacoity')
         OR level3_case_nature IN ('Motorcycle Theft', 'Car Theft', 'Cycle Theft', 'Other Vehicles Theft'))
    """,
    "crime_against_person": """
        (level2_case_nature IN ('Murder', 'Sexual Assault', 'Kidnapping / Abduction')
         OR level3_case_nature = 'Aerial Firing')
    """,
    "": """
        AND (level2_case_nature IN ('Vehicle Snatching', 'Burglary', 'Robbery/Snatching', 'Dacoity', 'Murder', 'Sexual Assault', 'Kiddnapping / Abduction')
         OR level3_case_nature IN ('Aerial Firing', 'Motorcycle Theft', 'Car Theft', 'Cycle Theft', 'Other Vehicles Theft'))
    """
}

district_eng_urdu = {
    "Attock": "اٹک",
    "Bahawalnagar": "بہاولنگر",
    "Bahawalpur": "بہاولپور",
    "Bhakkar": "بھکر",
    "Chakwal": "چکوال",
    "Chiniot": " چنیوٹ",
    "D. G. Khan": "ڈیرہ غازی خان",
    "Faisalabad": "فیصل آباد",
    "Gujranwala": "گوجرانوالہ",
    "Gujrat": "گجرات",
    "Hafizabad": "حافظ آباد",
    "Jhang": "جھنگ",
    "Jhelum": "جہلم",
    "Kasur": "قصور",
    "Khanewal": "خانیوال",
    "Khushab": "خوشاب",
    "Lahore": "لاہور",
    "Layyah": "لیہ",
    "Lodhran": "لودھراں",
    "M.B. Din": "منڈی بہاوالدین",
    "Mianwali": "میانوالی",
    "Multan": "ملتان",
    "Muzaffargarh": "مظفر گڑھ",
    "Nankana Sb": "ننکانہ صاحب",
    "Narowal": "نارووال",
    "Okara": "اوکاڑہ",
    "Pakpattan": "پاکپتن",
    "Rahimyar Khan": "رحیم یار خان",
    "Rajanpur": "راجن پور",
    "Rawalpindi": "راولپنڈی",
    "Sahiwal": "ساہیوال",
    "Sargodha": "سرگودھا",
    "Sheikhupura": "شیخوپورہ",
    "Sialkot": "سیالکوٹ",
    "T.T. Singh": "ٹوبہ ٹیک سنگھ",
    "Vehari": "وہاڑی"
}
