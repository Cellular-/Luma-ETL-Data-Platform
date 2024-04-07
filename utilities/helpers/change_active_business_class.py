"""
Modifies the active business class in config/app.config.

- jrgarrar
"""

# Library Imports
import os
import pathlib
import argparse
import logging
import logging.config
from config.config import setup_logging

# Globals
### A variable that points to this file's parent directory
PARENT_DIR = pathlib.Path(__file__).parent.absolute()
UTILITIES_DIR = pathlib.Path(PARENT_DIR).parent.absolute()
PROJECT_DIR = pathlib.Path(UTILITIES_DIR).parent.absolute()
CONFIG_DIR = os.path.join(PROJECT_DIR, "config")
CONFIG_FILE = os.path.join(CONFIG_DIR, "app.config")
CONFIG_BACKUP_FILE = os.path.join(CONFIG_DIR, "app.config.old")
setup_logging()

# Argument Parsing
def parse_args():
    """
    Parse input arguments and return them as a dict-like object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "new_business_class",
        help="The value that will replace the current business class",
    )
    args = parser.parse_args()
    return args

# Main Function
def main(args):
    """
    The main function.
    """
    # Open the config file
    with open(CONFIG_FILE, "r+") as f:
        config_contents = f.readlines()

        # Find the matching line
        active_line = [x for x in config_contents if "active =" in x][0]
        active_line_index = config_contents.index(active_line)

        # Swap the active value
        replacement_line = f"active = {args.new_business_class}\n"
        replacement_file = config_contents[:active_line_index] + [replacement_line] + config_contents[active_line_index+1:]
    
    # Save a copy of the current config file
    with open(CONFIG_BACKUP_FILE, "w") as f:
        f.write(''.join(config_contents))

    # Save the output to file
    with open(CONFIG_FILE, "w") as f:
        f.write(''.join(replacement_file))


# If called from the command line, parse arguments and run main()
if __name__ == "__main__":
    args = parse_args()
    main(args)
