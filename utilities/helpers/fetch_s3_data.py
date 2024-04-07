"""
Fetches a file from the output/ S3 bucket.

- jrgarrar
"""

# Library Imports
import os
import pathlib
import argparse
import logging
import logging.config
from utilities.aws.s3 import get_object
from config.config import setup_logging

# Globals
PARENT_DIR = pathlib.Path(__file__).parent.absolute()
UTILITIES_DIR = pathlib.Path(PARENT_DIR).parent.absolute()
PROJECT_DIR = pathlib.Path(UTILITIES_DIR).parent.absolute()
LANDING_DIR = os.path.join(PROJECT_DIR, 'landing-zone')
S3_BUCKET = "databrew"
S3_SUBDIRECTORY = "output"
setup_logging()

# bucket, subfolder, source_file_name, desination_file_name=''

# Argument Parsing
def parse_args():
    """
    Parse input arguments and return them as a dict-like object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "source_file_name",
        help="The name of the file in the S3 bucket (i.e. FSM_BillingInvoice_cleansed.csv).",
    )
    args = parser.parse_args()
    return args


# Main Function
def main(args):
    """
    The main function.
    """
    logging.info("Beginning Data Download...")

    logging.info(f"Target {args.source_file_name}")
    
    # Set target path
    target_path = os.path.join(LANDING_DIR, args.source_file_name)
    get_object(S3_BUCKET, S3_SUBDIRECTORY, args.source_file_name, target_path)
    logging.info("Finished Data Download...")


# If called from the command line, parse arguments and run main()
if __name__ == "__main__":
    args = parse_args()
    main(args)
