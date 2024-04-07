"""
Searches log files in a window for ERROR statements and compiles them into a
summary file.

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
LOG_DIR = os.path.join(PROJECT_DIR, 'logs')
SUMMARY_LOG_DIR = os.path.join(LOG_DIR, 'summaries')
setup_logging()

# Argument Parsing
def parse_args():
    """
    Parse input arguments and return them as a dict-like object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "key",
        help="A string used to filter log files by (i.e. '2023_05_01').",
    )
    args = parser.parse_args()
    return args

# Main Function
def main(args):
    """
    Run SQL stored procedures on the server.
    """
    # Grab all log files matching the provided string
    matching_filenames = [x for x in os.listdir(LOG_DIR) if args.key in x]

    # Configure headers for outputs
    duration_output = ['RUNTIMES\n']
    error_output = ['ERRORS\n']

    # Parse the log files 
    for filename in matching_filenames:
        # Add filename info to outputs
        error_output.append(f'>>>{filename}')
        error_output.append('\n')
        duration_output.append(f'>>>{filename}')
        duration_output.append('\n')

        # Parse logfiles
        full_filename = os.path.join(LOG_DIR, filename)
        with open(full_filename, encoding='utf-16') as f:
            contents = f.readlines()

            # Fetch duration messages
            duration_messages = [x.strip('LOAD DURATION:') for x in contents if 'LOAD DURATION:' in x]
            duration_output.extend(duration_messages)

            # Fetch error messages
            error_messages = [x for x in contents if 'load failed' in x.lower()]
            error_output.extend(error_messages)

        error_output.append('\n')

    # Output the results in a summary file
    output_filename = f'{args.key}_summary.log'
    with open(os.path.join(SUMMARY_LOG_DIR, output_filename), 'w+') as f:
        f.writelines(''.join(duration_output))
        f.writelines('\n\n\n')
        f.writelines(''.join(error_output))

# If called from the command line, parse arguments and run main()
if __name__ == "__main__":
    args = parse_args()
    main(args)
