# custom_logger.py
import logging

import mysql
import mysql.connector
from colorama import Fore, Style, init
from Utilities.db_config import get_db_connection
from datetime import datetime
from Utilities import configs

# Initialize colorama for color support on Windows
init(autoreset=True)

class CustomLogger:

    def __init__(self):
        """Initialize the logger with file and console handlers."""
        self.logger = logging.getLogger('Logs')
        self.logger.setLevel(logging.DEBUG)  # Set global logging level

        # Initialize the database connection
        self.db_connection = get_db_connection()
        if self.db_connection:
            print("Database connection established.")
        else:
            print("Failed to establish database connection.")

    def log_to_database(self, level, message):
        if self.db_connection:
            cursor = self.db_connection.cursor()
            query = "INSERT INTO 15_stats_logs (status, time_date, description) VALUES (%s, %s, %s)"
            data = (level, self.get_current_time(), message)
            try:
                cursor.execute(query, data)
                self.db_connection.commit()
            except mysql.connector.Error as err:
                print(f"Database Error: {err}")
                # Log the error
                self.logger.error(f"Failed to log to the database: {err}")
            finally:
                cursor.close()

    def get_current_time(self):
        return datetime.now().strftime(configs.YMD_HMS)

    def add_log(self, level, message):
        """Log a message and also save it to the database."""
        self.log_to_database(logging.getLevelName(level), message)

    def get_logger(self):
        """Return the logger instance for use in other files."""
        return self.logger

    def close(self):
        """Close the database connection when done."""
        if self.db_connection:
            self.db_connection.close()
