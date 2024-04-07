"""
Executes a specific SQL stored procedure.

- jrgarrar
"""

# Library Imports
import os
import argparse
import time
import pathlib
import logging
import logging.config
from db import database
from config.config import setup_logging

# Globals
PARENT_DIR = pathlib.Path(__file__).parent.absolute()
UTILITIES_DIR = pathlib.Path(PARENT_DIR).parent.absolute()
PROJECT_DIR = pathlib.Path(UTILITIES_DIR).parent.absolute()
# CONFIG_DIR = os.path.join(PROJECT_DIR, "config")
# CONFIG_FILE = os.path.join(CONFIG_DIR, "app.config")
DB_NAME = "SCOLumaStaging"
setup_logging()

# Argument Parsing
def parse_args():
    """
    Parse input arguments and return them as a dict-like object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "stored_procedure_name",
        help="The name of the stored procedure on the Staging server.",
    )
    parser.add_argument(
        "--fetch_results",
        help="Flag to toggle results output.",
        action="store_true"
    )
    args = parser.parse_args()
    return args

# Main Function
def main(args):
    """
    Run SQL stored procedures on the server.
    """
    start_time = time.perf_counter()

    # Configure a SQL connection
    conn = database.Database().connect()

    # Prep the query
    query = f'SET NOCOUNT ON; EXEC {DB_NAME}.dbo.{args.stored_procedure_name}'

    # Log the query
    logging.debug(query)

    # Run the query
    try:
        logging.info(f"Running [{DB_NAME}].[dbo].[{args.stored_procedure_name}]...")
        output = conn.execute(query)
        logging.info(f"Output: {output}")
    
    except Exception as e:
        logging.error(e)
        raise e

    end_time = time.perf_counter()
    dur = end_time - start_time
    logging.info(f"Stored Procedure Duration: {dur}")

# If called from the command line, parse arguments and run main()
if __name__ == "__main__":
    args = parse_args()
    main(args)
