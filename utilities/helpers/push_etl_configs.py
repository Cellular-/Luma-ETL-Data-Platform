"""
Pushes local ETL configurations up to S3.

- jrgarrar
"""

# Library Imports
import os
import pathlib
import logging
import logging.config
from utilities.aws.s3 import upload_file
from config.config import setup_logging

# Globals
PARENT_DIR = pathlib.Path(__file__).parent.absolute()
UTILITIES_DIR = pathlib.Path(PARENT_DIR).parent.absolute()
PROJECT_DIR = pathlib.Path(UTILITIES_DIR).parent.absolute()
CONFIG_DIR = os.path.join(PROJECT_DIR, "config")
APP_CONFIG_FILE = os.path.join(CONFIG_DIR, 'app.config')
BUSINESS_CLASS_MAPPING_FILE = os.path.join(CONFIG_DIR, "business_class_stage_table_mapping.json") 
BUSINESS_CLASS_NO_CLEANSE = os.path.join(CONFIG_DIR, 'business_class_no_cleansing.txt')
CONFIG_S3_BUCKET = 'etl-resources'
setup_logging()


# Main Function
def main():
    """
    The main function.
    """
    logging.info("Beginning Config Upload...")

    upload_file(APP_CONFIG_FILE, CONFIG_S3_BUCKET)
    upload_file(BUSINESS_CLASS_MAPPING_FILE, CONFIG_S3_BUCKET)
    upload_file(BUSINESS_CLASS_NO_CLEANSE, CONFIG_S3_BUCKET)

    logging.info("Finished Config Upload...")


# If called from the command line, parse arguments and run main()
if __name__ == "__main__":
    main()
