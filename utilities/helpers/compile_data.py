"""
Compiles CSV files of varying schemas, generating a summary file.

- jrgarrar
"""

# Library Imports
import pathlib
import argparse
import logging
import logging.config
from utilities.utilities import bc_merged_csv
from config.config import setup_logging

# Globals
PARENT_DIR = pathlib.Path(__file__).parent.absolute()
UTILITIES_DIR = pathlib.Path(PARENT_DIR).parent.absolute()
PROJECT_DIR = pathlib.Path(UTILITIES_DIR).parent.absolute()
setup_logging()

# Argument Parsing
def parse_args():
    """
    Parse input arguments and return them as a dict-like object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "business_class",
        help="The name of the business class (i.e. FSM_ReportingChartAccount).",
    )
    args = parser.parse_args()
    return args


# Main Function
def main(args):
    """
    The main function.
    """
    logging.info("Beginning Data Compilation...")

    logging.info(f"Target Business Class: {args.business_class}")

    # Run the merged csv function, executing the returned function
    try:
        bc_merged_csv(args.business_class)()
    except Exception as e:
        logging.info(e)
        raise e
    
    logging.info("Finished Data Compilation...")


# If called from the command line, parse arguments and run main()
if __name__ == "__main__":
    args = parse_args()
    main(args)
