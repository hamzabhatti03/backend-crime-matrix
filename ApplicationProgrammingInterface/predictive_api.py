# import datetime
from datetime import datetime ,timedelta
import pandas as pd
from Utilities import configs as conf
from Utilities import  db_config as db
import mysql.connector
from collections import defaultdict
import pymysql.cursors
import json

def yesterday_forecast_db(ps, district):
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

    target_date_for_actual = datetime.now() - timedelta(days=1)
    target_date_for_actual = target_date_for_actual.strftime('%Y-%m-%d')
    query_for_actual = """
            SELECT category, COUNT(*) AS count
            FROM pred_pol_preprocessed_crime_data
            WHERE ps_station_id = %s AND report_date = %s
            GROUP BY category;
            """
    target_date_for_predict = datetime.now() - timedelta(days=1)
    target_date_for_predict = target_date_for_predict.strftime('%d-%m-%Y')
    query_for_predict = """
                SELECT category, COUNT(*) AS count
                FROM pred_pol_predictions
                WHERE police_station= %s AND date = %s
                GROUP BY category;
                """
    query_for_today = """
                    SELECT category, COUNT(*) AS count
                    FROM pred_pol_predictions
                    WHERE police_station= %s AND date = %s
                    GROUP BY category;
                    """
    target_date_for_today = datetime.now()
    target_date_for_today = target_date_for_today.strftime('%d-%m-%Y')
    try:
        # Connect to the database
        connection = db.get_db_connection()
        cursor = connection.cursor()
        cursor.execute(query_for_actual, (police_station, target_date_for_actual))
        results_for_actual = cursor.fetchall()
        crime_counts_for_actual = {category.decode('utf-8') if isinstance(category, bytes) else category: count for category, count in results_for_actual}
        cursor = connection.cursor()
        cursor.execute(query_for_predict, (police_station, target_date_for_predict))
        results_for_predict = cursor.fetchall()
        crime_counts_for_predict = {category.decode('utf-8') if isinstance(category, bytearray) else category: count for category, count in results_for_predict}
        cursor = connection.cursor()
        cursor.execute(query_for_today, (police_station, target_date_for_today))
        results_for_today = cursor.fetchall()
        crime_counts_for_today = {category.decode('utf-8') if isinstance(category, bytearray) else category: count for category, count in results_for_today}

    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return {}

    finally:
        # Close the connection
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()

    yesterday_target_date = datetime.now() - timedelta(days=2)
    yesterday_target_date = yesterday_target_date.strftime('%Y-%m-%d')
    return {
        'yesterday_actual_count': crime_counts_for_actual,
        'yesterday_predicted_count': crime_counts_for_predict,
        'today_predicted_count':crime_counts_for_today
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


def get_category_data(ps,district):
    district = district
    ps = ps

    police_station = None
    district_id = None
    try:
        ps_id = None
        conn = db.get_db_connection()
        cursor = conn.cursor()
        district_id = conf.REVERSED_DISTRICTS_DICTIONARY.get(district)

        if ps is not None:
            query = f"SELECT id FROM 15_police_stations WHERE name = %s AND district_id = %s"
            cursor.execute(query, (ps, district_id))
            ps_id = cursor.fetchall()
            if ps_id:
                police_station = ps_id[0][0]
        cursor.close()
        conn.close()
    except Exception as e:
        print(e)

    if not district or not police_station:
        return {"error": "Both 'district' and 'police_station' parameters are required."}

    connection = db.get_db_connection()
    cursor = connection.cursor(dictionary=True)  # This makes cursor return dictionaries instead of tuples

    try:
        current_date = datetime.now().strftime('%d-%m-%Y')
        query = (
            "SELECT * FROM pred_pol_prediction "
            "WHERE district = %s AND police_station = %s AND date = %s"
        )
        cursor.execute(query, (district_id, police_station, current_date))
        rows = cursor.fetchall()

        # Column names to categorize the data
        columns = [
            "Assault/Hurt", "Vehicle Theft", "Robbery/Snatching", "Theft", "Kidnapping",
            "Traffic Accident", "Burglary", "Murder", "Religious Offences", "Dacoity"
        ]

        category_count = []
        predicted_categories = []

        # Loop through the rows returned from the query
        for row in rows:
            for i, category in enumerate(columns):
                # The column index starts at 3 since the first 3 columns are not category data
                column_name = list(row.keys())[i + 3]  # Get the actual column name
                category_data = row[column_name]

                if category_data:
                    parsed_data = json.loads(category_data)

                    # If data is a single item, wrap it in a list for uniformity
                    if isinstance(parsed_data, dict):
                        parsed_data = [parsed_data]

                    for item in parsed_data:
                        latitude = float(item.get("latitude", 0)) if item.get("latitude") else None
                        longitude = float(item.get("longitude", 0)) if item.get("longitude") else None

                        # Only append if both latitude and longitude are not None
                        if latitude is not None and longitude is not None:
                            category_count.append({
                                "category": item.get("category", category),
                                "latitude": latitude,
                                "longitude": longitude,
                                "location": item.get("location", ""),
                                "message": item.get("message", "")
                            })

                    # Check if the message is empty, if so set the count to 0
                    count = len(parsed_data) if parsed_data and parsed_data[0].get("message", "") != "" else 0

                    predicted_categories.append({
                        "category": category,
                        "count": count,
                        "message": parsed_data[0].get("message", "") if parsed_data else ""
                    })
                else:
                    # Handle cases where there is no data for a category
                    predicted_categories.append({
                        "category": category,
                        "count": 0,
                        "message": ""
                    })

        # Final result
        result = {
            "category_count": category_count,
            "predicted_categories": predicted_categories
        }
        return result

    except Exception as e:
        return {"error": str(e)}

    finally:
        cursor.close()
        connection.close()
