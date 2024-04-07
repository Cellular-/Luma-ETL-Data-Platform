"""
Pushes a file from business-classes/ up to an S3 bucket.

- jrgarrar
"""

# Library Imports
import os
import pathlib
import argparse
import logging
import logging.config
from utilities.aws.s3 import upload_file
from config.config import setup_logging

# Globals
PARENT_DIR = pathlib.Path(__file__).parent.absolute()
UTILITIES_DIR = pathlib.Path(PARENT_DIR).parent.absolute()
PROJECT_DIR = pathlib.Path(UTILITIES_DIR).parent.absolute()
BUSINESS_CLASS_DIR = os.path.join(PROJECT_DIR, "business-classes")
setup_logging()

# Argument Parsing
def parse_args():
    """
    Parse input arguments and return them as a dict-like object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "file_name",
        help="The name of the file to upload (i.e. FSM_ReportingChartAccount_all_schemas.csv).",
    )
    parser.add_argument(
        "tenant_name",
        help="The name of the tenant directory (i.e. AX5).",
    )
    parser.add_argument(
        "--bucket",
        help="The name of the S3 bucket (i.e. lumadw-datalake-raw-data).",
        default="lumadw-datalake-raw-data"
    )
    parser.add_argument(
        "--s3_path",
        help="Modified S3 path to use, if provided.",
    )
    parser.add_argument(
        "--skip_databrew",
        help="Flag to change the filename for pre-cleansed data.",
        action="store_true"
    )
    args = parser.parse_args()
    return args


# Main Function
def main(args):
    """
    The main function.
    """
    logging.info("Beginning Data Upload...")

    # Join paths
    target_file = args.file_name

    logging.info(f"Target Local File: {target_file}")
    logging.info(f"Target S3 Bucket: {args.bucket}")

    if args.skip_databrew:
        args.s3_path = args.s3_path.replace('_all_schemas.csv', '_cleansed.csv')
        upload_file(target_file, args.bucket, args.s3_path)
    else:
        upload_file(target_file, args.bucket, None)

    logging.info(f"Target S3 Path: {args.s3_path}")
    logging.info("Finished Data Upload...")

# If called from the command line, parse arguments and run main()
if __name__ == "__main__":
    args = parse_args()
    main(args)
