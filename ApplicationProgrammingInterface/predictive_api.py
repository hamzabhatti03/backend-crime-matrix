# import datetime
from datetime import datetime ,timedelta
import pandas as pd
from Utilities import configs as conf
from Utilities import  db_config as db
import mysql.connector
from collections import defaultdict
from psycopg2.extras import DictCursor
import pymysql.cursors
import json

def yesterday_forecast_db(ps, district,pg_conn):
    results_for_actual_dict = None
    police_station = None
    district_id = None

    # Define category configuration
    original_columns = [
        "Assault/Hurt", "Vehicle Theft", "Robbery/Snatching", "Theft", "Kidnapping",
        "Traffic Accident", "Burglary", "Murder", "Religious Offences", "Dacoity"
    ]
    desired_columns = [
        "Vehicle Theft", "Robbery/Snatching", "Theft", "Traffic Accident", "Burglary", "Dacoity"
    ]

    try:
        # Database setup and police station ID retrieval remains the same

        pg_curser = pg_conn.cursor()
        district_id = conf.REVERSED_DISTRICTS_DICTIONARY.get(district)

        if ps is not None:
            query = 'SELECT id FROM "15_police_stations" WHERE name = %s AND district_id = %s'
            pg_curser.execute(query, (ps, str(district_id)))
            ps_id = pg_curser.fetchall()
            police_station = ps_id[0][0] if ps_id else None

        # Get actual crime data from yesterday
        target_date_for_actual = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        pg_cursor = pg_conn.cursor()
        query_for_actual = """
            SELECT category, COUNT(*) AS count
            FROM preprocessed_crime_data
            WHERE ps_station_id = %s AND report_date = %s
            GROUP BY category;
        """
        pg_cursor.execute(query_for_actual, (police_station, target_date_for_actual))
        results_for_actual = pg_cursor.fetchall()

        # Process actual data and filter to desired categories
        results_for_actual_dict = {category: 0 for category in desired_columns}
        for category, count in results_for_actual:
            if isinstance(category, bytes):
                category = category.decode('utf-8')
            if category in desired_columns:
                results_for_actual_dict[category] = count

    except Exception as e:
        print(e)
        results_for_actual_dict = {category: 0 for category in desired_columns}

    # Initialize prediction data structures
    predicted_categories = {category: 0 for category in desired_columns}
    crime_counts_for_today = {category: 0 for category in desired_columns}

    try:
        # Get yesterday's predictions
        pg_cursor = pg_conn.cursor(cursor_factory=DictCursor)
        target_date_for_prediction = (datetime.now() - timedelta(days=1)).strftime('%d-%m-%Y')
        query = "SELECT * FROM prediction WHERE district = %s AND police_station = %s AND date = %s"
        pg_cursor.execute(query, (str(district_id), str(police_station), target_date_for_prediction))
        rows = pg_cursor.fetchall()

        # Process yesterday's predictions
        for row in rows:
            for category in desired_columns:
                original_idx = original_columns.index(category)
                column_name = list(row.keys())[original_idx + 3]
                category_data = row[column_name]

                if category_data:
                    parsed_data = json.loads(category_data)
                    parsed_data = [parsed_data] if isinstance(parsed_data, dict) else parsed_data
                    valid_entries = [item for item in parsed_data if item.get("message")]
                    predicted_categories[category] = len(valid_entries)

        # Get today's predictions
        pg_cursor = pg_conn.cursor(cursor_factory=DictCursor)
        target_date_for_today = datetime.now().strftime('%d-%m-%Y')
        pg_cursor.execute(query, (str(district_id), str(police_station), target_date_for_today))
        rows = pg_cursor.fetchall()

        # Process today's predictions
        for row in rows:
            for category in desired_columns:
                original_idx = original_columns.index(category)
                column_name = list(row.keys())[original_idx + 3]
                category_data = row[column_name]

                if category_data:
                    parsed_data = json.loads(category_data)
                    parsed_data = [parsed_data] if isinstance(parsed_data, dict) else parsed_data
                    valid_entries = [item for item in parsed_data if item.get("message")]
                    crime_counts_for_today[category] = len(valid_entries)

    except Exception as e:
        print(e)

    return {
        'yesterday_actual_count': results_for_actual_dict,
        'yesterday_predicted_count': predicted_categories,
        'today_predicted_count': crime_counts_for_today
    }



def aggregate_crime_data(data):
    # Use defaultdict to aggregate categories
    category_summary = defaultdict(lambda: {"count": 0, "message": ""})

    for item in data:
        category = item['category']
        # Update count
        category_summary[category]['count'] += item['count']
        # Use the first encountered message for each category
        if not category_summary[category]['message']:
            category_summary[category]['message'] = item['message']

    # Convert to list of dictionaries
    result = [
        {
            "category": cat,
            "count": details['count'],
            "message": details['message']
        }
        for cat, details in category_summary.items()
    ]

    return result


def process_dashboard_stats_db(ps=None, district=None):
    police_station = None
    try:
        ps_id = None
        conn = db.get_db_connection()
        curser = conn.cursor()
        district_id = conf.REVERSED_DISTRICTS_DICTIONARY.get(district)

        if ps is not None:
            query = f"SELECT id FROM 15_police_stations WHERE name ='{ps}' AND district_id = '{district_id}'"
            curser.execute(query)
            ps_id = curser.fetchall()
            police_station = ps_id[0][0]
    except Exception as e:
        print(e)

    category_count2 = []

    try:
        conn = db.get_db_connection()
        curser = conn.cursor()
        predict_query = "SELECT category, count, latitude, longitude, location, message FROM pred_pol_predictions WHERE date = %s AND police_station = %s"
        curser.execute(predict_query, (datetime.now().date().strftime('%d-%m-%Y'), police_station))
        Ps_data_db = curser.fetchall()
        columns = [desc[0] for desc in curser.description]
        df_Ps_data = pd.DataFrame(Ps_data_db, columns=columns)
        retrieved_data = df_Ps_data.to_dict(orient='records')
        normalized_data = [
            {key: (value.decode('utf-8') if isinstance(value, bytearray) else value)
             for key, value in entry.items()}
            for entry in retrieved_data
        ]

    except Exception as e:
        print(e)

    for row in normalized_data:
        category_count2.append({
            "category": row["category"],
            "count":  row["count"],
            "latitude": float(row["latitude"]),
            "longitude": float(row["longitude"]),
            "location": row["location"],
            "message": row["message"]
        })

    predicted_categories2 = aggregate_crime_data(normalized_data)

    return {
        'category_count' : category_count2 if category_count2 else None,
        'predicted_categories': predicted_categories2 if predicted_categories2 else None
    }


def get_category_data(ps, district, pg_conn):
    district = district
    ps = ps

    police_station = None
    district_id = None

    try:
        ps_id = None
        pg_cursor = pg_conn.cursor()
        district_id = conf.REVERSED_DISTRICTS_DICTIONARY.get(district)

        if ps is not None:
            query = 'SELECT id FROM "15_police_stations" WHERE name = %s AND district_id = %s'
            pg_cursor.execute(query, (ps, str(district_id)))
            ps_id = pg_cursor.fetchall()
            if ps_id:
                police_station = ps_id[0][0]
        pg_cursor.close()
    except Exception as e:
        print(e)

    if not district or not police_station:
        return {"error": "Both 'district' and 'police_station' parameters are required."}

    pg_cursor = pg_conn.cursor(cursor_factory=DictCursor)

    try:
        current_date = datetime.now().strftime('%d-%m-%Y')
        query = (
            "SELECT * FROM prediction "
            "WHERE district = %s AND police_station = %s AND date = %s"
        )
        pg_cursor.execute(query, (str(district_id), str(police_station), current_date))
        rows = pg_cursor.fetchall()

        # Original columns for index mapping
        original_columns = [
            "Assault/Hurt", "Vehicle Theft", "Robbery/Snatching", "Theft", "Kidnapping",
            "Traffic Accident", "Burglary", "Murder", "Religious Offences", "Dacoity"
        ]
        # Desired categories to display
        desired_columns = [
            "Vehicle Theft", "Robbery/Snatching", "Theft", "Traffic Accident", "Burglary", "Dacoity"
        ]

        category_count = []
        predicted_categories = []

        for row in rows:
            for category in desired_columns:
                original_index = original_columns.index(category)
                column_index = original_index + 3  # First 3 columns are non-category
                column_name = list(row.keys())[column_index]
                category_data = row[column_name]

                if category_data:
                    parsed_data = json.loads(category_data)

                    if isinstance(parsed_data, dict):
                        parsed_data = [parsed_data]

                    for item in parsed_data:
                        latitude = float(item.get("latitude", 0)) if item.get("latitude") else None
                        longitude = float(item.get("longitude", 0)) if item.get("longitude") else None

                        if latitude is not None and longitude is not None:
                            category_count.append({
                                "category": item.get("category", category),
                                "latitude": latitude,
                                "longitude": longitude,
                                "location": item.get("location", ""),
                                "message": item.get("message", "")
                            })

                    count = len(parsed_data) if parsed_data and parsed_data[0].get("message", "") != "" else 0

                    predicted_categories.append({
                        "category": category,
                        "count": count,
                        "message": parsed_data[0].get("message", "") if parsed_data else ""
                    })
                else:
                    predicted_categories.append({
                        "category": category,
                        "count": 0,
                        "message": ""
                    })

        result = {
            "category_count": category_count,
            "predicted_categories": predicted_categories
        }
        return result

    except Exception as e:
        return {"error": str(e)}

    finally:
       if pg_cursor:
            pg_cursor.close()


def forecast_date(ps, district, start_date, end_date,pg_conn):
    police_station = None
    district_id = None
    try:
        ps_id = None

        pg_cursor = pg_conn.cursor()
        district_id = conf.REVERSED_DISTRICTS_DICTIONARY.get(district)
        if ps is not None:
            query = 'SELECT id FROM "15_police_stations" WHERE name = %s AND district_id = %s'
            pg_cursor.execute(query, (ps, str(district_id)))
            ps_id = pg_cursor.fetchall()
            police_station = ps_id[0][0]
    except Exception as e:
        print(e)

    # Format dates for predictions
    start_date_prediction = start_date
    end_date_prediction = end_date

    # Get predicted category counts grouped by date
    predictions_by_date = {}


    try:
        pg_cursor = pg_conn.cursor(cursor_factory=DictCursor)

        query = (
            "SELECT * FROM prediction "
            "WHERE district = %s AND police_station = %s AND date BETWEEN %s AND %s"
        )
        pg_cursor.execute(query, (str(district_id), str(police_station), start_date_prediction, end_date_prediction))
        rows = pg_cursor.fetchall()

        # Column names to categorize the data
        # columns = [
        #     "Assault/Hurt", "Vehicle Theft", "Robbery/Snatching", "Theft", "Kidnapping",
        #     "Traffic Accident", "Burglary", "Murder", "Religious Offences", "Dacoity"
        # ]

        columns = [
            "Vehicle Theft", "Robbery/Snatching", "Theft", "Traffic Accident", "Burglary", "Dacoity"
        ]

        # Process each row
        for row in rows:
            # Decode bytearray fields to strings
            date = row['date']

            if date not in predictions_by_date:
                predictions_by_date[date] = {}

            for category in columns:
                # Get category data
                category_data = row.get(category, None)
                if category_data:
                    # Decode binary data to string
                    category_data_str = category_data

                    # Parse JSON data
                    parsed_data = json.loads(category_data_str)

                    # Ensure parsed data is a list for uniformity
                    if isinstance(parsed_data, dict):
                        parsed_data = [parsed_data]

                    # Count non-empty messages
                    count = len([item for item in parsed_data if item.get("message", "").strip() != ""])
                    predictions_by_date[date][category] = count
                else:
                    # Default to 0 if no data is present
                    predictions_by_date[date][category] = 0

    except mysql.connector.Error as err:
        print(f"Error getting predictions: {err}")
    finally:
        if pg_cursor:
            pg_cursor.close()

    return predictions_by_date
