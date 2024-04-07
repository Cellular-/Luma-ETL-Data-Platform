"""
Parses the config file and prints the active business class.

- jrgarrar
"""

# Library Imports
import config.config as cfg
import logging
import logging.config
from config.config import setup_logging

setup_logging()

# Main Function
def main():
    """
    The main function.
    """
    # Parse the config file at config/app.config
    config = cfg.create_config()

    # Fetch the active extraction group
    extraction_group = config['extractions']['active']

    # Fetch the corresponding business class
    active_business_class = config['extraction_groups'][extraction_group]

    # Print to terminal
    print(active_business_class)


# If called from the command line, parse arguments and run main()
if __name__ == "__main__":
    main()
