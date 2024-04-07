"""
Executes the SQL scripts in a directory.

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
setup_logging()

# Argument Parsing
def parse_args():
    """
    Parse input arguments and return them as a dict-like object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "target_directory",
        help="The directory containing SQL scripts for execution.",
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

    # Extract the contents of each script into a list
    table_generation_script_contents = []
    target_dir = os.path.join(PARENT_DIR, args.target_directory)
    file_list = os.listdir(target_dir)

    for filename in file_list:
        # If the filename ends in ".sql"...
        if '.sql' in filename:
            # Open it, then dump the contents into a list
            file_path = os.path.join(target_dir, filename)
            with open(file_path, 'r+') as f:
                script_contents = f.readlines()
                script_string = ''.join(script_contents)
                table_generation_script_contents.append(script_string)

    # Execute the SQL scripts one-by-one, storing output
    for script in table_generation_script_contents:
        logging.info('Executing script...')
        # Split apart scripts with "GO"
        # https://stackoverflow.com/questions/25680812/incorrect-syntax-near-go
        if 'GO' in script:
            split_scripts = script.split('GO')
            for s in split_scripts:
                logging.info(s)
                conn.execute(s)
        
        # Run other scripts as-is
        else:
            logging.info(script)
            conn.execute(script)
        logging.info('Script Executed...')
        logging.info('---')


# If called from the command line, parse arguments and run main()
if __name__ == "__main__":
    args = parse_args()
    main(args)
