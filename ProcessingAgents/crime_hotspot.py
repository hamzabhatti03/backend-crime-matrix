import math
import pymysql
import requests
from datetime import datetime
from math import isclose
from Utilities import db_config

def fetch_recent_cases(cursor):
    """Fetch cases from the last 20 minutes"""
    query = """
        SELECT * 
        FROM `15_preprocessed`
        WHERE `lat` IS NOT NULL AND `long` IS NOT NULL 
        AND level3_case_nature IS NOT NULL
        AND `time_id` >= UNIX_TIMESTAMP(NOW() - INTERVAL 20 MINUTE)
        ORDER BY `time_id` DESC;
    """
    cursor.execute(query)
    return cursor.fetchall()


def coordinates_match(lat1, long1, hotspot_coords, threshold=0.0001):
    """
    Check if coordinates match any point in hotspot dictionary
    Using threshold for floating point comparison
    """
    for coord_key, coord_data in hotspot_coords.items():
        for coord_pair in coord_data['coordinates']:
            if (isclose(float(lat1), float(coord_pair[0]), abs_tol=threshold) and
                    isclose(float(long1), float(coord_pair[1]), abs_tol=threshold)):
                return True, coord_key, coord_data['lead_ids']
    return False, None, None

def haversine_distance(coord1, coord2):
    """Calculate the Haversine distance between two points on the Earth."""
    lat1 = coord1[0]
    lon1 = coord1[1]
    lat2 = coord2[0]
    lon2 = coord2[1]

    # Convert latitude and longitude from degrees to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [float(lat1), float(lon1), float(lat2), float(lon2)])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Radius of Earth in kilometers
    r = 6371
    return c * r


def fetch_coordinates_within_radius(district_id, case_nature):
    conn = db_config.get_db_connection()
    cursor = conn.cursor()

    if not district_id or not case_nature:
        print({"error": "district_id and case_nature are required parameters"})
        return None

    try:
        if case_nature:
            query = """
            SELECT lat, `long`, lead_id
            FROM pred_pol_response_time
            WHERE complete_by = 'Responder' AND district_id = %s AND level3_case_nature = %s;
            """
        else:
            print({"message": "case_nature not found in level3_case_nature columns"})
            return None

        cursor.execute(query, (district_id, case_nature))

        results = cursor.fetchall()

        if not results:
            print({"message": "No coordinates found for the given parameters"})
            return None

        valid_results = [(float(lat), float(long), lead_id) for lat, long, lead_id in results if lat is not None and long is not None and lead_id is not None]

        coordinates_within_radius = {}
        used_coordinates = set()

        for base_coord in valid_results:
            base_lat, base_lon, base_lead_id = base_coord
            if (base_lat, base_lon) in used_coordinates:
                continue

            nearby_coords = []
            for coord in valid_results:
                coord_lat, coord_lon, coord_lead_id = coord
                if (coord_lat, coord_lon) not in used_coordinates and haversine_distance(base_coord, coord) <= 0.5:
                    nearby_coords.append((coord_lat, coord_lon, coord_lead_id))

            if len(nearby_coords) >= 4:
                coordinates_within_radius[f"{base_lat}, {base_lon}"] = {
                    "coordinates": [list(coord[:2]) for coord in nearby_coords],
                    "lead_ids": [coord[2] for coord in nearby_coords]
                }
                used_coordinates.update([(coord[0], coord[1]) for coord in nearby_coords])

        if not coordinates_within_radius:
            print({"message": "No hotspots found within the specified radius"})
            return None

        return coordinates_within_radius

    except Exception as e:
        print({"error": f"Database error: {e}"})
        return None

    finally:
        conn.close()


def process_cases():
    results = []

    conn = db_config.get_db_connection()
    try:
        with conn.cursor(dictionary=True) as cursor:
            print("Fetching recent cases...")

            # Fetch recent cases
            cases = fetch_recent_cases(cursor)

            for case in cases:
                try:
                    case_nature = case.get('level3_case_nature')
                    district = case.get('district_id')
                    police_station = case.get('police_station')
                    case_lat = case.get('lat')
                    case_long = case.get('long')
                    time_id = case.get('time_id')
                    location = case.get('caller_location')
                    lead_id = case.get('lead_id')

                    if not all([case_nature, district, police_station, case_lat, case_long, lead_id]):
                        continue

                    # Check if lead_id already exists
                    cursor.execute('''
                        SELECT 1 FROM pred_pol_crimes_hotspot WHERE lead_id = %s
                    ''', (lead_id,))
                    existing_case = cursor.fetchone()

                    if existing_case:
                        print(f"Lead ID {lead_id} already exists, skipping this record.")
                        continue  # Skip the insert if lead_id exists

                    # Fetch hotspots
                    hotspots = fetch_coordinates_within_radius(district, case_nature)

                    if hotspots:
                        matches, matched_coord, related_lead_ids = coordinates_match(case_lat, case_long, hotspots)

                        if matches and matched_coord:
                            matched_coordinates = hotspots.get(matched_coord, [])
                            # Extract lead_ids for the matched coordinates
                            related_case_ids = related_lead_ids # List of lead_ids

                            case_coords = f"{case_lat}, {case_long}"
                            date = datetime.fromtimestamp(int(time_id)).strftime('%d-%m-%Y') if time_id else 'N/A'
                            time = datetime.fromtimestamp(int(time_id)).strftime('%H:%M:%S') if time_id else 'N/A'

                            # Insert data into MySQL
                            cursor.execute('''
                                INSERT INTO pred_pol_crimes_hotspot (
                                    case_nature, lead_id, district, police_station,
                                    case_coords, matched_coordinates, time, date, location, related_cases
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ''', (
                                case_nature,
                                lead_id,
                                district,
                                police_station,
                                case_coords,
                                str(matched_coordinates),  # Convert list to string
                                time,
                                date,
                                location,
                                str(related_case_ids)  # Convert list of lead_ids to string
                            ))

                            # Prepare the response
                            results.append({
                                "case_nature": case_nature,
                                "lead_id": lead_id,
                                "district": district,
                                "police_station": police_station,
                                "case_coords": case_coords,
                                "matched_coordinates": matched_coordinates,
                                "related_case_ids": related_case_ids,  # Include related_case_ids
                                "time": time,
                                "date": date,
                                "location": location
                            })

                except Exception as e:
                    print(f"Error processing case: {e}")

            # Commit MySQL changes
            conn.commit()
            print("Cases processed and stored successfully.")
            return results

    except pymysql.MySQLError as mysql_err:
        print(f"MySQL Error: {mysql_err}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()
        print("MySQL connection closed.")


# Main Execution
if __name__ == '__main__':
    processed_results = process_cases()
    if processed_results:
        print("Processed Results:")
        for result in processed_results:
            print(result)