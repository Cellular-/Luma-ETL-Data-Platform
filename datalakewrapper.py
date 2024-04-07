import time
import logging
import logging.config
import requests as r, json, os, urllib.parse, utilities.utilities as util, argparse, sys
from oauth.datalakeoauth import OAuthResources, OAuthPayload, OAuthRequest, OAuthEndpoints
from datetime import datetime
from metadata import datalakemetadata as dlmd
from pathlib import Path
from resources.dictdefinitions import DatalakeExtractionContext
from typing import List
from abc import ABC, abstractmethod
from config.config import get_config, setup_logging

CONFIG = get_config()
setup_logging()      

class DatalakeEndpoints:
    _BASE    = 'https://mingle35-ionapi.inforgov.com/{tenant_name}/IONSERVICES/datalakeapi/v1'
    _BASE_V2 = 'https://mingle35-ionapi.inforgov.com/{tenant_name}/IONSERVICES/datalakeapi/v2'

    def __init__(self, tenant_name):
        self._BASE = self._BASE.format(tenant_name=tenant_name)
        self._BASE_V2 = self._BASE_V2.format(tenant_name=tenant_name)
        self.DATA_OBJECT_PROPERTIES = self._BASE + '/payloads/list?records={num_records}&filter={filter}'
        self.DATA_OBJECT_PROPERTIES_SPLIT = self._BASE_V2 + '/payloads/splitquery?filter={filter}'
        self.DATA_OBJECT_BY_ID = self._BASE + '/payloads/streambyid?datalakeId={id}'

class FSMEndpoints:
    _BASE    = 'https://mingle35-ionapi.inforgov.com/IDAHO_PRD/FSM/fsm/soap'

    def __init__(self, tenant_name):
        self._BASE = self._BASE.format(tenant_name=tenant_name)
        self.GENERIC_LIST = self._BASE_V2 + 'classes/GLTransactionDetail/lists/_generic?_fields=_all&_limit=5'

class DatalakeQuery(ABC):
    """
    Represents a query to the datalake and serves as base class for
    subclassing.
    """
    def __init__(self, oauth_request: OAuthRequest, datalake_endpoints: DatalakeEndpoints):
        self.oauth_request = oauth_request
        self.datalake_endpoints = datalake_endpoints
        self.last_response = None
        self.data = None

    @abstractmethod
    def query(self):
        pass

    def process(self, filepath):
        with open(f'{filepath}', 'w') as file:
            json.dump(self.data, file)

        return self

class DataObjectProperties(DatalakeQuery):
    """
    Represents a datalake objects properties. The properties of a datalake object
    contain data objects, their IDs and other information. For a given business class,
    there are multiple data objects with IDs which compose the data object properties.

    See an example of a data object's properties in `business-classes/AX5/FSM_Contract.json`.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.datalake_endpoints = self.datalake_endpoints
        self.obj_gen = None

    def query_split(self, filter):
        """
        Query the endpoint for a business class that returns filters that
        can be used to query data object properties in chunks.
        """
        url = self.datalake_endpoints.DATA_OBJECT_PROPERTIES_SPLIT.format(
            filter=f'({urllib.parse.quote(filter)})'
        )

        response = r.get(
            url,
            headers = {'Authorization': f'Bearer {self.oauth_request.oauth_token.access_token}'}
        )

        if response.status_code != 200:
            raise Exception(response.text)
        
        return json.loads(response.content.decode("utf-8"))

    def query(self, filter, batch_size=10000):
        url = self.datalake_endpoints.DATA_OBJECT_PROPERTIES.format(
            filter=f'({urllib.parse.quote(filter)})',
            num_records=batch_size
        )

        response = r.get(
            url,
            headers = {'Authorization': f'Bearer {self.oauth_request.oauth_token.access_token}'}
        )

        if response.status_code != 200:
            raise Exception(response.text)
        
        return json.loads(response.content.decode("utf-8"))


        pass

class DatalakeOptions:
    """
    Represents the available command line options to datalakewrapper.py
    and stores their True or False values for easier access.
    """
    def __init__(self, generate_schemas: bool, extract_data: bool, incremental_load: bool, full_load: bool):
        self.generate_schemas = generate_schemas
        self.extract_data = extract_data
        self.incremental_load = incremental_load
        self.full_load = full_load

class BusinessClass:
    def __init__(self, name=None, schemas=None, data=None):
        self.name = name
        self.schemas = schemas
        self.data = data

class DatalakeServiceBase:
    """
    Composes all dependencies to interact with datalake.
    """
    def __init__(self, 
                config,
                dl_endpoints : DatalakeEndpoints,
                oauth_endpoints : OAuthEndpoints,
                oauth_resources : OAuthResources,
                oauth_request : OAuthRequest,
                data_obj_props : DataObjectProperties,
                opts : DatalakeOptions):

        self.config = config
        self.filenames = dict(self.config.items('filename_templates'))
        self.extraction_groups = self.config.get('extractions', 'active').split('\n')
        self.bc_iter = self.__get_business_classes()
        self.bc_filter = util.create_filter('dl_document_name')('eq')
        self.curr_bc = None
        self.dl_endpoints = dl_endpoints
        self.oauth_endpoints = oauth_endpoints,
        self.oauth_resources = oauth_resources
        self.oauth_request = oauth_request
        self.query_props = {
                "oauth_request": oauth_request, 
                "datalake_endpoints": dl_endpoints
            }
        self.data_obj_props = data_obj_props(**self.query_props)
        self.opts = opts
        self.extraction_start_time = None

        self.__init_environment()

    def __init_environment(self):
        self.extraction_start_time = datetime.now().strftime('%Y%m%d%H%M%S')

    def __get_business_classes(self):
        for eg in self.extraction_groups:
            yield self.config.get('extraction_groups', eg).split('\n')

    @util.write_to_schema_file
    @util.extract
    def query_data(self, object_id: str):
        """
        Queries the datalake object by ID endpoint. This function can be modified to process
        the response from the datalake using decorator functions. Decorator functions can 
        process and write the data.

        object_id -- id of object in datalake
        """
        
        return r.get(
            self.dl_endpoints.DATA_OBJECT_BY_ID.format(id=object_id),
            headers = {'Authorization': f'Bearer {self.oauth_request.oauth_token.access_token}'}
        )

    def post_extract_process(func):
        def wrapper(self, *args, **kwargs):
            context = func(self, *args, **kwargs)

            # Update schemas json file
            with open(context['filename_templates']['schemas'].format(business_class=self.curr_bc.name), 'w') as f:
                f.write(json.dumps(self.curr_bc.schemas))
            
            # Write extracted ids to history
            if self.opts.incremental_load:
                with open(self.filenames['bc_inc_extraction_history'].format(business_class=self.curr_bc.name), 'a') as f:
                    f.write(''.join(context['ids_extracted']))
                
            with open(self.filenames['bc_extraction_history'].format(business_class=self.curr_bc.name), 'a') as f:
                f.write(''.join(context['ids_extracted']))
        return wrapper

    def post_extract_validate(func):
        def wrapper(self, *args, **kwargs):
            context = func(self, *args, **kwargs)
            validation_results = util.record_count_by_bc(self.curr_bc.name)
            file_rec_count, expected_count, match = validation_results['file_extract_record_count'],\
                                                    validation_results['expected_record_count'],\
                                                    validation_results['match']
            
            message = f"{self.curr_bc.name}: Record counts on datalake documents, {expected_count},{' DO NOT' if not match else ''} match raw data, {file_rec_count}, in versioned files!"
            logging.info(message)

            # Pass context to next function in post-extract phase.
            return context
        return wrapper
    
    def compile_data_obj_props(self, business_class: str = None):
        """
        Queries the payload split endpoint which returns a set of datalake query filters
        which are used to query the datalake for data object properties in chunks.
        The chunks are then grouped together. This grouping makes up the list of ids
        that are needed to fetch data from datalake.
        """
        filters = [f['queryFilter'].replace('(', '').replace(')','') for f in self.data_obj_props.query_split(self.bc_filter(self.curr_bc.name))]
        self.data_obj_props.data = self.data_obj_props.query(filters.pop(0))
        for f in filters:
            data = self.data_obj_props.query(f)
            for object_id in data['fields']:
                self.data_obj_props.data['fields'].append(object_id)
    
    def set_up(self, bc=None):
        if not bc:
            bc = self.curr_bc

        # Write data object properties to file.
        self.compile_data_obj_props()
        self.data_obj_props.process(self.filenames['obj_props'].format(business_class=bc.name))

        # Load data object properties from file into memory.
        with open(self.filenames['obj_props'].format(business_class=bc.name), 'r') as file:
            obj_props = json.load(file)
            ids = (obj['dl_id'] for obj in obj_props['fields'])

        """
        These actions set up the business class depending on the type of load. Usually and incremental
        or full load.
        """
        util.create_columns_file(business_class=bc.name)
        dlmd.new_dl_metadata_instance()\
            .query(bc.name, False).to_csv(self.filenames['bc_metadata_filename'].format(bc_name=bc.name))
        
        # Set up incremental extract folder structure
        folder_name = self.filenames['inc_data_active_id']\
            .format(bc_folder=bc.name, active_inc_id=util.get_active_inc_id())
        Path(folder_name).mkdir(parents=True, exist_ok=True)
        
        if self.opts.incremental_load:
            ids = util.not_extracted_ids(business_class=bc.name)
        elif self.opts.full_load:
            util.remove_data_by_schema_files(business_class=bc.name)
            # util.reset_schema_file(business_class=bc.name)
            util.clear_extract_history(business_class=bc.name)

        util.create_versioned_files(business_class=bc.name)

        bc.schemas = util.get_schemas(bc.name)

        return ids

    @post_extract_process
    # @post_extract_validate
    def process_bc(self) -> DatalakeExtractionContext:
        """
        Iterates through the object ids of the current business class and
        extracts data from datalake using those ids.

        Returns a dict containing useful context for the next function in the 
        post-extract phase. 
        """
        ids = self.set_up(self.curr_bc)
        ids_as_list = list(ids)
        id_count = sum(1 for val in ids_as_list)
        logging.info(f"Found {id_count} ids...")

        ids_extracted = []
        counter = 0

        for id in ids_as_list:
            logging.debug(id)

            try:
                self.query_data(object_id=id)
            except Exception as e:
                logging.error(f'Error querying data for object id: {id}')
                with open(f'tmp/{self.curr_bc.name}_ids_extracted.csv', 'a+') as f:
                    f.write(''.join(ids_extracted))
                continue
            else:
                ids_extracted.append(f'{id}\n')
                if counter % 100 == 0 or counter == id_count-1:
                    logging.info(f"Processing id {counter} / {id_count-1}...")
                counter += 1

        return {
            'business_class': self.curr_bc,
            'ids_extracted': ids_extracted,
            'filename_templates': self.filenames
        }

    def process_multiple_bc(self):
        for eg in self.extraction_groups:
            logging.info(f'Processing extraction group: {eg}')
            for bc in self.config.get('extraction_groups', eg).split('\n'):
                logging.info(f'Processing bc: {bc}')
                self.curr_bc = BusinessClass(name=bc)
                self.process_bc()

def main():
    """
    Create all dependencies for datalake servers. Load configuration values.
    """
    logging.info("Beginning Infor extraction...")
    logging.debug(args)
    start_time = time.perf_counter()

    tenant_name = CONFIG.get('env_vars', 'active_tenant')
    datalake_endpoints = DatalakeEndpoints(tenant_name=tenant_name)
    oauth_endpoints = OAuthEndpoints(tenant_name=tenant_name)
    oauth_resources = OAuthResources(CONFIG.get('filename_templates', 'oauth_credentials'))
    oauth_payloads = OAuthPayload(oauth_resources)
    oauth_request = OAuthRequest(oauth_payloads, oauth_endpoints)
    data_obj_props = DataObjectProperties
    dl_options = DatalakeOptions(
        **{
            'generate_schemas': args.gs,
            'extract_data': args.ed,
            'incremental_load': args.il,
            'full_load': args.fl
        }
    )

    try:
        """
        Datalake service class used to extract business classes as
        defined in the config/app.config file extraction_groups and
        extractions sections.
        
        If the service is unable to generate/refresh the oauth token
        then the script is exited since the token is required for any 
        interactions with the datalake
        """
        dl_service = DatalakeServiceBase(
            config=CONFIG,
            dl_endpoints=datalake_endpoints,
            oauth_endpoints=oauth_endpoints,
            oauth_resources=oauth_resources,
            oauth_request=oauth_request,
            data_obj_props = data_obj_props,
            opts=dl_options
        )
    except SystemExit as e:
        logging.error(f'Could not get OAuth token.\n{e.message}')
    else:
        try:
            dl_service.process_multiple_bc()
        except Exception as e:
            logging.error(e)
            raise e
    
    end_time = time.perf_counter()
    dur = end_time - start_time
    logging.info(f"Infor Extraction Duration: {dur}")
    logging.info("Infor extraction complete...")

if __name__ == '__main__':
    # Parse input arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--gs', help='Generate schemas from datalake', action='store_true')
    parser.add_argument('--ed', help='Extract data from datalake', action='store_true')
    parser.add_argument('--il', help='Perform incremental data extraction', action='store_true')
    parser.add_argument('--fl', help='Perform full wipe/replace data extraction', action='store_true')
    args = parser.parse_args()
    logging.info(args)

    # Run main
    main()