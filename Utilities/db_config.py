# db_config.py
import os
import mysql.connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def get_db_connection():
    """Create and return a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(

            host=os.getenv('DB_HOST'),

            database=os.getenv('DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        if connection.is_connected():
            # print("Successfully connected to the database.")
            return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def get_vccs_db_connection():
    """Create and return a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(

            host=os.getenv('DB_HOST'),

            database=os.getenv('VCCS_DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        if connection.is_connected():
            # print("Successfully connected to the database.")
            return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def get_vwps_db_connection():
    """Create and return a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(

            host=os.getenv('DB_HOST'),
            database=os.getenv('VWPS_DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        if connection.is_connected():
            # print("Successfully connected to the database.")
            return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def get_vcm_db_connection():
    """Create and return a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(

            host=os.getenv('DB_HOST'),
            database=os.getenv('VCM_DB_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        if connection.is_connected():
            # print("Successfully connected to the database.")
            return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

def get_blood_db_connection():
    """Create and return a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(

            host=os.getenv('DB_HOST'),
            database=os.getenv('DB_BLOOD_NAME'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD')
        )
        if connection.is_connected():
            # print("Successfully connected to the database.")
            return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None