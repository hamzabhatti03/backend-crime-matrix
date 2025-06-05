# db_config.py
import os
import mysql.connector
from dotenv import load_dotenv
import psycopg2

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


def get_15_staging_db_connection():
    """Create and return a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(

            host=os.getenv('STAGING_HOST'),

            database=os.getenv('STAGING_DB'),
            user=os.getenv('STAGING_USER'),
            password=os.getenv('STAGING_PASSWORD')
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


def get_chat_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('CHAT_DB'),
            user=os.getenv('USER_CHAT'),
            password=os.getenv('PASSWORD_CHAT'),
            host=os.getenv('HOST_CHAT'),
            port=os.getenv('PORT_CHAT')
        )
        return conn
    except Exception as e:
        print(e)
        raise


def get_notification_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv('NOTIFICATION_DB'),
            user=os.getenv('USER_CHAT'),
            password=os.getenv('PASSWORD_CHAT'),
            host=os.getenv('HOST_CHAT'),
            port=os.getenv('PORT_CHAT')
        )
        return conn
    except Exception as e:
        print(e)
        raise

def get_pg_logdb_connection():
    try:
        connection = psycopg2.connect(
            host=os.getenv('PG_PROD_HOST'),
            database=os.getenv('LOGS_DB_NAME'),
            user=os.getenv('PG_PROD_USER'),
            password=os.getenv('PG_PROD_PASSWORD')
        )
        return connection
    except psycopg2.Error as err:
        print(f"Production PostgreSQL Connection Error: {err}")
        return None

def get_1787_db_connection():
    """Create and return a connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(

            host=os.getenv('COMPLAINTS_1787_HOST'),

            database=os.getenv('COMPLAINTS_1787_DB'),
            user=os.getenv('COMPLAINTS_1787_USER'),
            password=os.getenv('COMPLAINTS_1787_PASSWORD'),
            port=3306
        )
        if connection.is_connected():
            # print("Successfully connected to the database.")
            return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

