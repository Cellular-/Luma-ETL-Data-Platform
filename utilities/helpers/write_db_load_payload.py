from config import config
from utilities.utilities import write_db_load_payload
import argparse
import logging
import logging.config

cfg = config.get_config()
config.setup_logging()

# Argument Parsing
def parse_args():
    """
    Parse input arguments and return them as a dict-like object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--business_class",
        help="The name of the business class (i.e. FSM_ReportingChartAccount).",
    )

    args = parser.parse_args()
    return args

# Main Function
def main(args):
    """
    The main function.
    """
    filename = write_db_load_payload(args.business_class)

    logging.info(f'Writing the database load payload to {filename}')

# If called from the command line, parse arguments and run main()
if __name__ == "__main__":
    args = parse_args()
    main(args)
