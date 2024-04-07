"""
Generates a CSV file that maps Infor business classes to replication sets.

- jrgarrar
"""

# Library Imports
import os
import argparse
import time
import json
import pathlib
import logging
import logging.config
from db import database
from config.config import setup_logging

# Globals
PARENT_DIR = pathlib.Path(__file__).parent.absolute()
UTILITIES_DIR = pathlib.Path(PARENT_DIR).parent.absolute()
PROJECT_DIR = pathlib.Path(UTILITIES_DIR).parent.absolute()
RESOURCE_DIR = os.path.join(PROJECT_DIR, 'resources')
MAPPING_FILE = os.path.join(RESOURCE_DIR, 'table_configuration_mappings.json')
OUTPUT_FILE = os.path.join(RESOURCE_DIR, 'bc_to_rep_set_mapping.csv')
setup_logging()

# Argument Parsing
def parse_args():
    """
    Parse input arguments and return them as a dict-like object.
    """
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    return args

# Main Function
def main(args):
    """
    
    """
    # Extract business class / rep set pairs from file
    with open(MAPPING_FILE, 'r') as f:
        mapping_data = json.load(f)
        records = [(
            mapping_data[k]['business_class_name'], 
            mapping_data[k]['replication_set_name'],
            mapping_data[k]['incremental'],
            mapping_data[k]['staging_table_name']
            ) 
            for k in mapping_data.keys()]

    # Output pairs as a CSV
    with open(OUTPUT_FILE, 'w+') as f:
        # Header
        f.write("BusinessClass,ReplicationSet,IsIncremental,StagingTable\n")
        # Contents
        for record in records:
            f.write(f"{record[0]},{record[1]},{record[2]},{record[3]}\n")

# If called from the command line, parse arguments and run main()
if __name__ == "__main__":
    args = parse_args()
    main(args)
