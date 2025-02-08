import pandas as pd
from Utilities.utils import get_processed_db_connection
import os
import glob


def create_fir_murder_psrms_table():
    create_table_query = """
    CREATE TABLE IF NOT EXISTS fir_murder_psrms (
        id SERIAL PRIMARY KEY,
        serial_no INTEGER,
        police_station VARCHAR(255),
        police_circle VARCHAR(255),
        district VARCHAR(255),
        fir_no VARCHAR(255),
        fir_datetime TEXT,
        section TEXT,
        incident_time TEXT,
        victim_name_with_details TEXT,
        item_type TEXT,
        item_value NUMERIC(12,2),
        recovered_item_value NUMERIC(12,2),
        accused TEXT,
        unknown_accused TEXT,
        arrested TEXT,
        wanted TEXT,
        inv_officer TEXT,
        status TEXT,
        incident_place TEXT,
        fir_description TEXT,
        longitude DOUBLE PRECISION,
        latitude DOUBLE PRECISION,
        crime_sub_head  TEXT NULL
    );
    """

    conn = None
    try:
        conn = get_processed_db_connection()
        cursor = conn.cursor()
        cursor.execute(create_table_query)
        conn.commit()
        print("Table 'fir_murder_psrms' created successfully.")
        cursor.close()
    except Exception as e:
        print(f"Error creating table: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def load_excel_files_to_db(excel_files, table_name, column_mapping):
    """
    Reads all sheets from a list of Excel files, renames the columns based on the provided mapping,
    and inserts the data into the specified PostgreSQL table.

    Parameters:
        excel_files (list): List of Excel file paths.
        table_name (str): Name of the target database table.
        column_mapping (dict): Dictionary mapping Excel column names (Urdu) to
                               database column names (English).
    """
    for file in excel_files:
        try:
            # Load the entire Excel file, reading all sheets into a dictionary of DataFrames
            xls = pd.ExcelFile(file, engine='openpyxl')
            sheet_names = xls.sheet_names  # Get all sheet names

            for sheet in sheet_names:
                try:
                    # Read the sheet into a DataFrame
                    df = pd.read_excel(xls, sheet_name=sheet, engine='openpyxl')

                    # Rename columns according to the mapping
                    df.rename(columns={col: column_mapping[col] for col in df.columns if col in column_mapping},
                              inplace=True)

                    # Replace NaN values with None to ensure compatibility with database NULL values
                    df = df.where(pd.notnull(df), None)

                    if 'longitude' in df.columns and 'latitude' in df.columns:
                        df['longitude'] = pd.to_numeric(df['longitude'],errors='coerce')
                        df['latitude'] = pd.to_numeric(df['latitude'],errors='coerce')
                        df = df.dropna(subset=['longitude','latitude'])
                    else:
                        print(f"skipping sheet {sheet} in {file} due to missing lat long")
                        continue
                    # Convert the DataFrame into a list of tuples for batch insertion
                    data_tuples = [tuple(row) for row in df.values]

                    # Prepare the column list and SQL placeholders for the INSERT query
                    columns = list(df.columns)
                    columns_joined = ', '.join(columns)
                    placeholders = ', '.join(['%s'] * len(columns))
                    insert_query = f"INSERT INTO {table_name} ({columns_joined}) VALUES ({placeholders})"

                    # Establish a database connection using your provided function
                    conn = get_processed_db_connection()
                    cursor = conn.cursor()

                    # Execute batch insert for improved performance
                    cursor.executemany(insert_query, data_tuples)

                    # Commit the transaction
                    conn.commit()
                    cursor.close()
                    conn.close()

                    print(f"Data from {file} (Sheet: {sheet}) successfully inserted into {table_name}.")

                except Exception as sheet_error:
                    print(f"Error processing sheet '{sheet}' in file '{file}': {sheet_error}")
                    if conn:
                        conn.rollback()

                finally:
                    if cursor:
                        cursor.close()
                    if conn:
                        conn.close()

        except Exception as file_error:
            print(f"Error processing file '{file}': {file_error}")

# --- Setup File Paths and Execution ---

# Determine the project root directory. Assuming this script is in 'Processing_agent' folder,
current_dir = os.path.dirname(os.path.abspath(__file__))

# The project root is assumed to be one level up from the current directory
project_root = os.path.join(current_dir, '..')

# Define the path to the 'fir_data' folder located in the project root
fir_data_folder = os.path.join(project_root, 'fir_data')

# Fetch all Excel files from the 'fir_data' folder regardless of their names
excel_files = glob.glob(os.path.join(fir_data_folder, "*.xlsx"))

# Define the mapping from Excel (Urdu) column names to database (English) column names.
column_mapping = {
    "نمبر شمار": "serial_no",
    "تھانہ": "police_station",
    "نام سرکل" : "police_circle",
    "ضلع" : "district",
    "مقدمہ نمبر" : "fir_no",
    "تاریخ وقت رپورٹ" : "fir_datetime",
    "بجرم" : "section",
    "تاریخ وقت وقو ع" : "incident_time",
    "نام مدعی معہ رابطہ نمبر" : "victim_name_with_details",
    "قسم مال" : "item_type",
    "مالیت مسروقہ" : "item_value",
    "مالیت بازیافتہ" : "recovered_item_value",
    "ملزمان نامزد" : "accused",
    "ملزمان  نامعلوم" : "unknown_accused",
    "ملزمان گرفتار" : "arrested",
    "ملزمان فراری" : "wanted",
    "نام تفتیشی" : "inv_officer",
    "پوزیشن" : "status",
    "جائے وقوعہ" : "incident_place",
    "متن ایف آئی آر" : "fir_description",
    "طول بلد" : "longitude",
    "عرض بلد" : "latitude",
    "کرائم سب ہیڈ": "crime_sub_head"
}

# Specify the target table name in PostgreSQL.
table_name = 'fir_murder_psrms'

# Load the Excel files into the database
create_fir_murder_psrms_table()
load_excel_files_to_db(excel_files, table_name, column_mapping)
