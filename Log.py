import logging
from constants import LOG_FILE

def setup_logger(log_file_path):
    logging.basicConfig(
        level=logging.INFO,  # Set the logging level to INFO, you can use DEBUG, INFO, WARNING, ERROR, CRITICAL
        format='%(asctime)s [%(levelname)s] %(message)s',  # Define the log message format
        datefmt='%Y-%m-%d %H:%M:%S',  # Define the date format for the log messages
        handlers=[
            logging.FileHandler(log_file_path),  # Add a FileHandler to save log messages to a file
            logging.StreamHandler(),  # Add a StreamHandler to also display log messages in the console
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logger(LOG_FILE)