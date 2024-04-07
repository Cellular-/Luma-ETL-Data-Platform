from datalakewrapper import DatalakeQuery
from oauth.datalakeoauth import new_oauth_object
import requests as r, json, argparse
from config.config import get_config
import definitions as defs
import os
import logging

class DatalakeCatalogEndpoints:
    _BASE = 'https://mingle35-ionapi.inforgov.com/{tenant_name}/IONSERVICES/datacatalog/v1/object/'

    def __init__(self, tenant_name):
        self.DATA_OBJECT_METADATA = self._BASE.format(tenant_name=tenant_name) + '{object_name}'

class DatalakeMetadata(DatalakeQuery):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.endpoint = self.datalake_endpoints.DATA_OBJECT_METADATA

    def query(self, object_name: str, debug=True):
        """
        Queries the datalake metadata endpoint for the given
        object (usually a business class). Can be chained into
        `to_csv` to write the metadata to a json file.
        """
        url = self.endpoint.format(object_name=object_name)

        response = r.get(
            url,
            headers = {'Authorization': f'Bearer {self.oauth_request.oauth_token.access_token}'}
        )

        if response.status_code == 200:
            data = json.loads(response.content.decode("utf-8"))
            self.data = data['schema']['properties']
            if debug:
                logging.debug(data)

        return self

    def format_schema_metadata(self, func, sep=',', filter=None):
        """
        Pass a function that will iterate through the schema metadata properties
        and format the data as desired.
        """
        return func(self.data, sep=sep)

    def to_csv(self, filepath: str):
        """
        Writes the metadata retrieved from the api to file.

        filepath - metadata destination filepath
        """
        with open(filepath, 'w') as f:
            f.write(json.dumps(self.data))

def new_dl_metadata_instance(active_tenant: str = None) -> 'DatalakeMetadata':
    """
    Returns new instance of DatalakeMetadata.
    """
    dl_metadata = DatalakeMetadata(
    **{
        "oauth_request": new_oauth_object(get_config(), tenant_name=active_tenant or get_config().get('env_vars', 'active_tenant')), 
        "datalake_endpoints": DatalakeCatalogEndpoints(tenant_name=active_tenant or get_config().get('env_vars', 'active_tenant'))
    })

    return dl_metadata

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--object_name', help='Name of datalake object')
    parser.add_argument('--sep', help='Column value delimiter')
    args = parser.parse_args()

    tenant_name = get_config().get('env_vars', 'active_tenant')
    bc_metadata_filename = get_config().get('filename_templates', 'bc_metadata_filename')

    endpoints = DatalakeCatalogEndpoints(tenant_name=tenant_name)
    dl_metadata = DatalakeMetadata(
        **{
            "oauth_request": new_oauth_object(get_config(), tenant_name=tenant_name), 
            "datalake_endpoints": endpoints
        })
    dl_metadata.query(args.object_name).to_csv(os.path.join(defs.ROOT_DIR, bc_metadata_filename.format(bc_name=args.object_name)))

    # TODO: Consider replacing print with pretty-print for better formatting
    dl_metadata.format_schema_metadata(print, sep=args.sep or ',')