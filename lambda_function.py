"""
Data loading script.

Pulls a CSV file from S3, performs lightweight munging, and loads it into SQL.

- slineses and jrgarrar
"""

import os
import json
import boto3
import time
import logging
import logging.config
import pathlib
import pprint
import argparse
import traceback
import numpy as np
import pandas as pd
import warnings
import db.database as database
from config.config import get_config, setup_logging

# Globals #####################################################################
# Directories when run locally
PARENT_DIR = pathlib.Path(__file__).parent.absolute()
RESOURCES_DIR = os.path.join(PARENT_DIR, "resources")
SQL_KEYWORD_FILE = os.path.join(RESOURCES_DIR, "sql_reserved_kw.txt")
SQL_TO_PYTHON_MAPPING_FILE = os.path.join(RESOURCES_DIR, "sql_to_python_mapping.json")
ROW_INSERT_CHUNK_SIZE = int(get_config().get('database', 'insert_chunk_size'))
setup_logging()

# Formatting
pd.set_option("display.float_format", lambda x: "%.2f" % x)

# S3
s3_client = boto3.resource("s3")

# SQL
insert_query = None


# Argument Parsing ############################################################
def parse_args():
    """
    Parse input arguments and return them as a dict-like object.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("s3_bucket", help="The name of the S3 bucket.")
    parser.add_argument(
        "csv_filepath",
        help="The name of the CSV file within the S3 bucket.",
    )
    parser.add_argument("db_table_name", help="The name of the database table.")
    parser.add_argument("mode", help="The mode used when loading (replace).")
    args = parser.parse_args()
    return args


# Helper Functions ############################################################
def get_sql_types(table_name):
    """
    Reaches out to the staging database and pulls down the schema.
    """
    mssql_db = database.Database().connect()
    mssql_db._cursor.execute(f"exec sp_columns {table_name}")

    col_type = {}
    for row in mssql_db._cursor:
        col_type[row[3]] = row[5]

    mssql_db._cursor.close()
    mssql_db._conn.close()

    return col_type


def col_py_types(col_sql_types, sql_py_type_map):
    """
    Convert a set of SQL types into Python types.
    """
    converted_types = {}

    for col, sql_type in col_sql_types.items():
        converted_types[col] = sql_py_type_map[sql_type]

    return converted_types


# Data Processing Functions ###################################################
def setup(s3_bucket, csv_filepath, verbose=False):
    """
    Pull down and connect to external resources (i.e. S3 data, SQL)
    """
    # Load in SQL reserved keywords
    with open(SQL_KEYWORD_FILE, "r") as f:
        sql_keyword_list = f.readlines()
        sql_keyword_list = [x.strip() for x in sql_keyword_list]

    # Load in SQL to Python type mapping
    with open(SQL_TO_PYTHON_MAPPING_FILE, "r") as f:
        sql_to_python_dict = json.load(f)

    # Prep S3 connection to input data
    s3_object = s3_client.Object(s3_bucket, csv_filepath)

    # Prep SQL connection output data location
    mssql_db = database.Database()

    return sql_keyword_list, sql_to_python_dict, s3_object, mssql_db


def data_import(db_table_name, s3_object, sql_to_python_dict, verbose=False):
    """
    Import data from S3 with the appropriate schema.
    """
    # Configure data types prior to import
    db_name, prefix, table_name = db_table_name.split(".")
    dtype = col_py_types(get_sql_types(table_name), sql_to_python_dict)
    logging.debug(dtype)

    # Note "datetime" columns and flip their types for read_csv()
    date_cols = [k for k in dtype.keys() if dtype[k] == 'datetime']
    logging.debug(date_cols)
    for k, v in dtype.items():
        if v == 'datetime':
            dtype[k] = 'string'

    # Connect to S3
    logging.info(s3_object)
    data = s3_object.get()["Body"]

    # Load CSV as a dataframe, catching warnings without writing to STDERR
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore')
        df = pd.read_csv(data, dtype=dtype, low_memory=True, verbose=verbose)


    # Noting missing columns (in metadata but not in data)
    columns = list(df.columns)
    logging.debug(columns)
    missing_columns = [x for x in dtype.keys() if x not in columns]
    if len(missing_columns) > 0:
        logging.warning(f"Logging columns visible in metadata but not present in actual data: {missing_columns}")
        date_cols = [x for x in date_cols if x not in missing_columns]
        dtype = {k: v for k in dtype if k not in missing_columns}
        # logging.warning(pprint.pprint(dtype))

    # Noting missing columns (in data but not in metadata)
    missing_columns = [x for x in columns if x not in dtype.keys()]
    if len(missing_columns) > 0:
        logging.warning(f"Logging columns visible in data but not present in metadata: {missing_columns}")
        c = {k: v for k in columns if k not in missing_columns}
        # logging.warning(pprint.pprint(c))
        # TODO: Find a better fix than dropping data
        for k in missing_columns:
            df.drop(k, inplace=True, axis=1)
    
    # Converting specific columns to datetimes, swapping them to NaT when they error out
    logging.info(f"Converting date columns for use by Pandas: {date_cols}")
    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    return df


def data_munge(df, sql_keyword_list):
    """
    Clean the incoming data by removing duplicate values, scrubbing nulls,
    and performing similar operations.
    """
    # Drop duplicate columns as necessary
    dupe_columns = [col for col in df.columns if "_dupe" in col]
    df.drop(dupe_columns, axis=1, inplace=True)

    # Format column names to avoid overlap with SQL reserved keywords
    remap_columns = {}
    for col in df.columns:
        remap_columns[col] = f"[{col}]"
    df.rename(columns=remap_columns, inplace=True)

    # Handle null values
    # Must be done with care in order to avoid triggering a Pandas bug
    # https://github.com/pandas-dev/pandas/issues/25288
    for col in df.columns:
        col_dtype = str(df[col].dtype)
        if col_dtype == "Int64":
            df[col].fillna(-99, inplace=True)
        elif col_dtype == "float64":
            df[col].fillna(-99.0, inplace=True)
        elif col_dtype == "string":
            df[col].fillna("", inplace=True)
        elif col_dtype == "Datetime64":
            df[col].fillna(np.datetime64("1900-01-01"), inplace=True)
        elif col_dtype == "datetime64[ns]":
            df[col].fillna(np.datetime64("1900-01-01"), inplace=True)
        elif col_dtype == "datetime64[ns, UTC]":
            df[col].fillna(pd.Timestamp("1970-01-01T00:00:00Z"), inplace=True)

def data_load(df, mssql_db, db_table_name, mode, verbose=False, debug=False):
    """
    Loads a Pandas dataframe into an MS SQL server.
    """
    # Drop existing table, if configured to do so
    if mode == "replace":
        query = f"truncate table {db_table_name}"
        logging.debug(query)
        mssql_db.execute(query)

    # Insert records in chunks
    global insert_query
    insert_query = (
        "insert into {full_table_name} ({columns}) values ({parameters})".format(
            full_table_name=db_table_name,
            columns=",".join([col for col in df.columns.tolist()]),
            parameters=f"{','.join(['?'] * len(df.columns))}",
        )
    )
    if verbose:
        logging.info(insert_query)
        logging.debug(debug)

    # If there are no records to load, notify and move on 
    df_row_count = len(df.index)
    if df_row_count == 0:
        logging.info(f"Loading 0 / 0 records...")
    
    # In normal mode, execute query
    elif not debug:
        logging.info(f"Loading {df_row_count} records...")

        df_breaks = [x for x in range(0, df_row_count, ROW_INSERT_CHUNK_SIZE)]
        df_breaks.append(df_row_count)
        logging.debug(df_breaks)

        for i in range(1, len(df_breaks)):
            if i != len(df_breaks):
                r = (df_breaks[i-1], df_breaks[i] - 1)
            else:
                r = (df_breaks[i-1], df_breaks[i])
            logging.info(f"Loading records {r[0]} to {r[1]}...")
            mssql_db.executemany(insert_query, df.iloc[r[0]: r[1] + 1].values.tolist())
    
    # In debug mode, execute query but do root-cause-analysis if it fails
    else:
        try:
            mssql_db.executemany(insert_query, df.values.tolist())
        except:
            bad_columns = {}
            truncate_statement = f"TRUNCATE TABLE {db_table_name}"
            logging.error('Insert Failed. Testing Columns...')

            # For every column, insert data individually and check for errors
            for col in df.columns.tolist():
                # Prep the query and parameters
                q = (f"INSERT INTO {db_table_name} ({col}) VALUES (?)")
                d = [(x,) for x in df[col].values.tolist()]

                # Log the test setup
                logging.debug(f'Testing {col}...')
                logging.debug(q)
                logging.debug(d)

                # Execute, then truncate afterwards to keep the table small
                try:
                    mssql_db.executemany(q, d)
                    mssql_db.execute(truncate_statement)

                # If there's an error, note the issue and log the offending column
                except Exception as e:
                    logging.error(f'ERROR: {e}')
                    bad_columns[col] = e
            
            # Report the offending columns and their errors as a dictionary
            logging.info(pprint.pprint(bad_columns))

def load_csv_into_db_table(
    s3_bucket: str, csv_filepath: str, db_table_name: str, mode: str
) -> None:
    """
    Loads a csv files from the specified s3 bucket into the corresponding
    database table.
    """
    start_time = time.perf_counter()
    logging.info(
        f"Beginning run for {s3_bucket}/{csv_filepath} to {db_table_name} with mode {mode}..."
    )

    # Setup
    logging.info("Performing setup...")
    sql_keyword_list, sql_to_python_dict, s3_object, mssql_db = setup(
        s3_bucket, csv_filepath
    )

    # Data Import
    logging.info("Performing data import...")
    df = data_import(db_table_name, s3_object, sql_to_python_dict)

    # Data Cleaning
    logging.info("Performing data munge...")
    data_munge(df, sql_keyword_list)

    # Data Load
    logging.info("Performing data load...")
    data_load(df, mssql_db, db_table_name, mode, verbose=True, debug=False)

    end_time = time.perf_counter()
    dur = end_time - start_time
    logging.info(f"Staging Load Duration: {dur}")
    
    logging.info("Run complete...")


# Lambda Handler ##############################################################
def lambda_handler(event, context):
    # Parse input arguments
    s3_bucket = event["s3_bucket"]
    csv_filepath = event["csv_filepath"]
    db_table_name = event["db_table_name"]
    mode = event["mode"]

    # Run the main function
    try:
        load_csv_into_db_table(s3_bucket, csv_filepath, db_table_name, mode)

    # If there are exceptions, report back with debugging information
    except Exception as e:
        exception_type = e.__class__.__name__
        message = f"Error for csv, {s3_bucket}/{csv_filepath}, and table, {db_table_name}: {str(e)}"

        # Missing S3 bucket
        if exception_type == "NoSuchKey":
            message = f"{message}\n{csv_filepath} does not exist in {s3_bucket} bucket."

        # Bad query
        elif exception_type == "ProgrammingError":
            if "Incorrect syntax" in message:
                message = f"{message}\nQuery Information: {insert_query}"
            elif "would be truncated" in message:
                message = f"{message}\nColumn Specification incorrect. Check the table definition."

        # Generic error
        error_object = {
            "is_error": True,
            "status_code": 400,
            "error_type": exception_type,
            "message": f"{message}\n{traceback.print_exc()}",
        }
        logging.error(error_object)
        return error_object

    # Otherwise, send confirmation of successful load
    else:
        success_object = {
            "is_error": False,
            "status_code": 200,
            "message": json.dumps(
                f"CSV File, {s3_bucket}/{csv_filepath}, loaded into {db_table_name} successfully!"
            ),
        }
        logging.info(success_object)
        return success_object


# Main function when executed locally
if __name__ == "__main__":
    # Parse input arguments
    args = parse_args()

    # Run main
    try:
        logging.info(args)
        load_csv_into_db_table(
            args.s3_bucket, args.csv_filepath, args.db_table_name, args.mode
        )
    except Exception as e:
        logging.info(e)
        raise e
