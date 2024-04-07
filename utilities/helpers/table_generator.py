"""
A script for creating table generation scripts for LUMA.

- jrgarrar
"""

# Library Imports
import os
import json
import pathlib
import argparse
import logging 
import logging.config
from jinja2 import Template
from datetime import datetime
from config.config import setup_logging

# Globals
PARENT_DIR = pathlib.Path(__file__).parent.absolute()
UTILITIES_DIR = pathlib.Path(PARENT_DIR).parent.absolute()
PROJECT_DIR = pathlib.Path(UTILITIES_DIR).parent.absolute()
RESOURCE_DIR = os.path.join(PROJECT_DIR, "resources")
STORED_PROC_DIR = os.path.join(PROJECT_DIR, "stored-procedures")
TABLE_GEN_DIR = os.path.join(STORED_PROC_DIR, "table_generation_scripts")
TABLE_MAPPING_FILE = os.path.join(RESOURCE_DIR, "table_configuration_mappings.json")
TEMPLATE_FILE = os.path.join(RESOURCE_DIR, "table_gen_template.sql")
setup_logging()

# Helper Functions
def parse_args():
    """
    Parse input arguments and return them as a dict-like object.
    """
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    return args


def fill_template(source_table_name):
    """
    Loads a template file in and fills in the blanks with provided arguments.
    """
    # Setup
    date = datetime.now().strftime("%Y/%m/%d")

    # Open template
    with open(TEMPLATE_FILE, "r") as f:
        template_list = f.readlines()
        template_str = "".join(template_list)
        template = Template(template_str)
        template_filled = template.render(
            date=date,
            source_table_name = source_table_name,
            target_table_name = source_table_name.replace('_dl', ''),
            proc_name = source_table_name.replace('luma_dl_', '') + '_proc'
        )
    
    return template_filled


def write_generated_file(filled_template, source_table_name):
    """
    Takes a filled out template and writes it to file.
    """
    # Derive the target table name
    target_table_name = source_table_name.replace('luma_dl_', '')

    # Write the finished file
    output_filename = f"{target_table_name}_proc.sql"
    target_path = os.path.abspath(os.path.join(TABLE_GEN_DIR, output_filename))
    with open(target_path, "w") as f:
        f.writelines(filled_template)
        logging.info(f"---{output_filename} Created---")
        logging.info(f"{target_path}")


# Main Function
def main(args):
    """
    Create SQL scripts using configurations.
    """
    # Load in list of tables
    with open(TABLE_MAPPING_FILE, 'r') as f:
        table_mappings = json.load(f)
    tables = [table_mappings[x]['staging_table_name'] for x in table_mappings.keys()]

    # Generate stored procedures for each table
    for table in tables:
        filled_template = fill_template(table)
        write_generated_file(filled_template, table)


# If called from the command line, parse arguments and run main()
if __name__ == "__main__":
    args = parse_args()
    main(args)
