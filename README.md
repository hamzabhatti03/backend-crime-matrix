#### api_pgs_optimized_v3.py
Api code return PUNJAB TODAY Data from CENTRALIZED POSTGRESQL DB
Latest file before this file is api_psg_optimized_v2.py, This file includes the changes i.e.,
1) LOGIN with HRMIS API
2) IGP INSIGHTS API
3) Updated and Modified total calls and cases count to match it with pucar-15 stats


#### api_pgs_optimized_v4_prod_copy.py
API code to incorporate Production Database Server and Migrated All APis to the Production Server
1) just need to include APis missing in this file from api_pgs_optimized_v3.py like (igp_insights)
2) Db for logs and notifications are also configured already


#### pgs_processing_script_v3.py
Code processing file to incorporate PUNJAB TODAY PROCESSING LOGIC USING CENTRALIZED POSTGRESQL DB
Latest file before this file for processing is pgs_processing_script_v2.py, which includes the optimization changes i.e.,
1) Removed extra tables not used further
2) Caller feedback in response_time table to resolve the dependency from main db
3) User activity logs mechanism updated
4) updated code for response_time columns that were not getting updated earlier from main db (district_id,police_station,level1_case_nature etc.)


#### pgs_processing_script_v4.py
Code processing file to incorporate PUNJAB TODAY PROCESSING LOGIC USING CENTRALIZED POSTGRESQL DB
Latest file before this file for processing is pgs_processing_script_v3.py, which includes updated total calls and total cases changes and migrated to production DBi.e.,
1) Migrated All tables to production server
2) updated total calls query in processed_data
3) Updated FIR scraping logic to fetch data for last 10 days