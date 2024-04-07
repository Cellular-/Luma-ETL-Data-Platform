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
        db = database.Database()
        table_name = bc_table_map[bc_name]
        delete_table_query = DDL.delete_table(database_name, table_name)
        logging.info(f"Attempting to delete table {table_name} for business class, {bc_name} if it exists...")
        db.execute(delete_table_query)

if __name__ == '__main__':
    main()