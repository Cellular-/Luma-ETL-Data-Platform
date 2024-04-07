"""
Adds a record to the job tracker SQL table.

- jrgarrar
"""

# Library Imports
import os
import argparse
import pathlib
import logging
import logging.config
from db import database
from config.config import setup_logging

# Globals
PARENT_DIR = pathlib.Path(__file__).parent.absolute()
UTILITIES_DIR = pathlib.Path(PARENT_DIR).parent.absolute()
PROJECT_DIR = pathlib.Path(UTILITIES_DIR).parent.absolute()
CONFIG_DIR = os.path.join(PROJECT_DIR, "config")
CONFIG_FILE = os.path.join(CONFIG_DIR, "app.config")
JOB_TABLE = 'job_tracker'
setup_logging()

# Argument Parsing
def parse_args():
    """
    Parse input arguments and return them as a dict-like object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "business_class",
        help="The name of a business class.",
    )
    parser.add_argument(
        "timestamp",
        help="The time of the job to be recorded.",
    )
    parser.add_argument(
        "was_successful",
        help="Whether or not the job was successful (true or false)",
    )
    parser.add_argument(
        "duration",
        help="Amount of time required for job to complete.",
    )
    parser.add_argument(
        "--create_job_table",
        help="Flag to create the SQL job table.",
        action="store_true"
    )
    args = parser.parse_args()
    return args

# Main Function
def main(args):
    """
    Run SQL scripts from the output directory.
    """
    # Configure a SQL connection
    conn = database.Database().connect()

    # If the SQL table does not exist, create it
    if args.create_job_table:
        query = f"""
        CREATE TABLE 
            [SCOLumaStaging].[dbo].[{JOB_TABLE}]
        ( 
            BusinessClass VARCHAR(50),
            Timestamp DATETIME2,
            WasSuccessful VARCHAR(10),
            Duration Time
        );
        """
        logging.info(query)
        output = conn.execute(query)
        logging.info(output)

    # Add the record to the SQL table
    query = f"""
    INSERT INTO
        {JOB_TABLE} 
    VALUES (
        '{args.business_class}',
        CONVERT( DATETIME2, '{args.timestamp}', 120 ),
        '{args.was_successful}',
        '{args.duration}'
        );
    """
    logging.debug(query)
    output = conn.execute(query)
    logging.debug(output)


# If called from the command line, parse arguments and run main()
if __name__ == "__main__":
    args = parse_args()
    main(args)
