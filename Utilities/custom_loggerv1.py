# custom_logger.py
import logging
from colorama import Fore, Style, init

# Initialize colorama for color support on Windows
init(autoreset=True)


class CustomLogger:
    """A reusable logger class with both file and colored console logging."""

    # Define log colors for different levels
    COLORS = {
        logging.DEBUG: Fore.YELLOW,
        logging.INFO: Fore.GREEN,
        logging.WARNING: Fore.BLUE,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.MAGENTA,
    }

    def __init__(self, log_file):
        """Initialize the logger with file and console handlers."""
        self.logger = logging.getLogger('Logs')
        self.logger.setLevel(logging.DEBUG)  # Set global logging level

        # File Handler (No colors)
        file_handler = logging.FileHandler(log_file)
        file_format = logging.Formatter(
            '%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
            datefmt='[%d/%b/%Y %I:%M:%S %p]')  # Set date format using 12-hour format with AM/PM, milliseconds and Removed %(name)s
        file_handler.setFormatter(file_format)
        self.logger.addHandler(file_handler)

        # Console Handler (With colors)
        console_handler = logging.StreamHandler()
        console_format = self.CustomFormatter(
            '%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
            datefmt='[%d/%b/%Y %I:%M:%S %p]')  # Set date format using 12-hour format with AM/PM, milliseconds and Removed %(name)s
        console_handler.setFormatter(console_format)
        self.logger.addHandler(console_handler)

    class CustomFormatter(logging.Formatter):
        """Formatter for adding colors to console logs based on log level."""

        def format(self, record):
            log_color = CustomLogger.COLORS.get(record.levelno, Fore.WHITE)
            message = super().format(record)
            return f"{log_color}{message}{Style.RESET_ALL}"

    def get_logger(self):
        """Return the logger instance for use in other files."""
        return self.logger
