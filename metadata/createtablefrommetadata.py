import sys

sys.path.append("..")

from db import database
import json
import logging
from config.config import get_config
from db.sql import DDL
from metadata.datalakemetadata import DatalakeMetadata, DatalakeCatalogEndpoints
from oauth.datalakeoauth import new_oauth_object
from utilities.utilities import filter_metadata, format_col_name, active_extraction_groups_bc
import definitions as defs, os

def main():
    # Set up required variables from configuration file.
    tenant_name = get_config().get('env_vars', 'active_tenant')
    bc_table_map_filename = get_config().get('data_resources', 'bc_table_map_filename')
    bc_metadata_filename = get_config().get('filename_templates', 'bc_metadata_filename')
    business_classes = active_extraction_groups_bc() or [table for table in get_config().get('database_tables', 'create_tables_for').split('\n')]
    database_name = 'SCOLumaStaging'

    with open(bc_table_map_filename, 'r') as f: 
        bc_table_map = json.load(f)

    dl_metadata = DatalakeMetadata(
    **{
        "oauth_request": new_oauth_object(get_config(), tenant_name=tenant_name), 
        "datalake_endpoints": DatalakeCatalogEndpoints(tenant_name=tenant_name)
    })
    db = database.Database()

    for bc_name in business_classes:
        if bc_name not in bc_table_map.keys():
            logging.error(f'No key, value pair in business class to table mapping for {bc_name}')
            logging.error(f'Add "{bc_name}": "<DATABASE_TABLE_NAME>" to {bc_table_map_filename}')

            continue

        db = database.Database()
        dl_metadata.query(bc_name).to_csv(os.path.join(defs.ROOT_DIR, bc_metadata_filename.format(bc_name=bc_name)))

        table_name = bc_table_map[bc_name]
        with open(os.path.join(defs.ROOT_DIR, bc_metadata_filename.format(bc_name=bc_name)), 'r') as md:
            logging.info(f'Attempting to create table {table_name} for business class, {bc_name}')

            metadata = json.load(md)
            if not metadata:
                logging.error(f'No metadata found for {bc_name}')
                continue

            mdf = filter_metadata(metadata, bc_name) # Filter bc metadata first so that create table statement only uses columns specified by users

            mdf_clean_cols = {}
            for col in mdf.keys():
                mdf_clean_cols[format_col_name(col)] = mdf[col]

            create_table_query = DDL.create_table(database_name, table_name, mdf_clean_cols)
            db.execute(create_table_query)

if __name__ == '__main__':
    main()