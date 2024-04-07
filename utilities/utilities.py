from utilities.aws import s3
import logging
import logging.config
import glob, json, pandas as pd, re, os, definitions as defs, functools, time, datetime
from config.config import get_config
from functools import partial
from typing import List, Callable
import metadata.types
from resources.dictdefinitions import BusinessClassDataBySchema, BusinessClassRecordCount, BusinessClassMetadata, BusinessClassPyType

"""
Several utility functions for performing various tasks when extracting
and moving data from the datalake and external databases/AWS resources.

Many of these functions use values defined in the config/app.config file
which drives the behavior of the application.
"""

def root_dir() -> str:
    """
    Returns a function that takes in a list of paths and joins them
    to the root directory of this applcation.
    """
    def sub_dir(sub_dirs: list[str]):
        return os.path.join(defs.ROOT_DIR, sub_dirs)
    return sub_dir

def clear_extract_history(business_class: str) -> None:
    """
    Clears the extract history for a specific business class.
    """
    filename = get_config().get('filename_templates', 'bc_extraction_history').format(business_class=business_class)
    if not os.path.exists(filename):
        with open(filename, 'x') as f: pass
    else:
        with open(filename, 'w') as f: pass

def bc_record_count_files(business_class: str) -> List[int]:
    """
    Returns list of the count of lines in each of a business classes data file by
    schema.
    """

    def read_file(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                yield line.strip()

    def count_lines(lines):
        count = 0
        for line in lines:
            count += 1
        yield count

    tots = []
    for file in glob.glob(get_config().get('filename_templates', 'bc_data_by_schema').format(business_class=business_class, version='*')):
        lines = read_file(file)
        line_count = count_lines(lines)
        tots.append(next(line_count))
        
    return tots

def bc_object_props(business_class: str) -> dict:
    with open(get_config().get('filename_templates', 'obj_props').format(business_class=business_class), 'r') as file:
        data_property_counts = json.load(file)

    return data_property_counts

def record_count_by_bc(business_class: str) -> BusinessClassRecordCount:
    """
    Compares number of lines in files containing the raw business class data
    to the number of records indicated by the data object property. If these
    counts don't match then there was a problem in the extraction process.
    """
    tots = bc_record_count_files(business_class)
    data_property_counts = bc_object_props(business_class)
        
    return {
        'file_extract_record_count': sum(tots),
        'expected_record_count': sum([record['dl_instance_count'] for record in data_property_counts['fields']]),
        'match': sum([record['dl_instance_count'] for record in data_property_counts['fields']]) == sum(tots)
    }

def get_columns(filepath: str) -> List[str]:
    """
    Returns the columns of a business class specified by the user.
    If a filepath is not specified, uses filename template from config.
    If file does not exist, return empty list.

    filepath -- path to columns file
    """
    try:
        with open(filepath or get_config().get('filename_templates', 'columns_to_load'), 'r') as file:
            return file.readlines()
    except FileNotFoundError as e:
        return []

def resolved_columns(business_class: str) -> List[str]:
    # This file is populated by the user. The user manually lists the names of the columns to load instead of loading
    # all of the columns.
    columns_file = os.path.join(defs.ROOT_DIR, get_config().get('filename_templates', 'columns_to_load')).format(business_class=business_class)
    
    # Load the user specified columns or all of the columns.
    all_columns = get_columns(columns_file) or get_columns_from_schemas(business_class)
    all_columns = resolve_col_issues(all_columns)

    return all_columns

def format_col_name(col_name: str) -> str:
    """
    Returns formatted column name compatible with sql server naming conventions.

    col_name -- column name
    """
    return col_name.replace('\n', '').replace('.', '_').replace('[', '').replace(']', '')

def create_columns_file(business_class: str) -> str:
    """
    Creates the file that specifies with columns from a business class
    should be processed/loaded.
    """
    try:
        desired_cols = get_columns(get_config().get('filename_templates', 'columns_to_load').format(business_class=business_class))
    except FileNotFoundError as e:
        with open(get_config().get('filename_templates', 'columns_to_load').format(business_class=business_class), 'w') as f: pass

    return get_config().get('filename_templates', 'columns_to_load').format(business_class=business_class)

def get_schemas(business_class: str, filename: str = '') -> dict:
    """
    Returns a business classes schemas.

    business_class -- datalake business class name
    filename       -- if empty, use default schema filename
    """
    if not filename:
        filename = get_config().get('filename_templates', 'schemas').format(business_class=business_class)

    if not os.path.exists(filename):
        with open(filename, 'w') as f:
            f.write('{"0": []}')
    
    with open(filename, 'r') as f:
        return json.load(f)

def reset_schema_file(business_class: str, filename: str = ''):
    if not filename:
        filename = get_config().get('filename_templates', 'schemas').format(business_class=business_class)

    if not os.path.exists(filename):
        with open(filename, 'w') as f:
            pass
    with open(filename, 'w') as f:
        f.write('{"0": []}')

def metadata_to_list(metadata: BusinessClassMetadata, sep: str):
    """
    Used to format business class metadata returned by the metadata API
    endpoint.

    schema -- schema object from datalake metadata endpoint
    sep    -- column separator (`,`, `|`, `-`)
    """
    for column in metadata:
        max_column = [prop for prop in metadata[column].keys() if prop.startswith('max')]
        max_column_val = metadata[column][max_column[0]] if max_column else 'N/A'
        datatype, col_name, length = (
            metadata[column]['type'] if 'type' in metadata[column].keys() else 'N/A', 
            column, 
            max_column_val
        )
        logging.debug(f'{col_name}{sep}{datatype}{sep}{length}')

def read_lines(filename, encoding='utf-8'):
    with open(filename, 'r', encoding=encoding) as f:
        for line in f:
            yield line

def count_lines(lines):
    count = 0
    for line in lines:
        count += 1
    yield count

def remove_data_by_schema_files(business_class: str):
    """
    Removes the data by schema files. These are files containing business class data
    appended with `v1`, `v2`, etc indicating the schema version for which the data
    belongs.

    This should be invoked if full extraction is performed.
    """
    for file in glob.glob(get_config().get('filename_templates', 'bc_data_by_schema').format(business_class=business_class, version='*')):
        os.remove(file)

def create_versioned_files(business_class: str) -> List[str]:
    """
    Creates placeholder csv based on the different schemas of the
    business class. Returns the list of created files.
    """

    data_by = inc_data_by_schema_filename(business_class) if is_incremental(business_class) else data_by_schema_filename(business_class)
    schemas = get_schemas(business_class)

    for version, schema in schemas.items():
        with open(data_by(version=version), 'w', encoding='utf-8') as f: pass

def not_extracted_ids(business_class: str) -> set:
    """
    Returns the set of data object IDs that have not yet been
    extracted from the datalake. Useful for performing incremental
    extracts from the datalake.

    business_class -- the business class to find the ids not yet extracted
    """
    with open(get_config().get('filename_templates', 'bc_extraction_history').format(business_class=business_class), 'r') as f:
        extracted_ids = [id.replace('\n', '') for id in f.readlines()]
    
    with open(get_config().get('filename_templates', 'obj_props').format(business_class=business_class), 'r') as f:
        obj_props = json.load(f)

    ids_from_dl = (obj['dl_id'] for obj in obj_props['fields'])

    return set(ids_from_dl) - set(extracted_ids)

def create_filter(property: str):
    """
    Create a datalake document filter. Filter can be partially created at first.

    property -- datalake object property name (e.g., dl_document_name)
    operator -- keyword to compare property and value (e.g., eq)
    value    -- value to filter the property on
    """
    def operator(operator):
        def filter_value(value):
            return f"{property} {operator} '{value}'"
        return filter_value
    return operator

def get_data_filenames() -> List[str]:
    """
    Returns a list of all business class related filenames.
    """
    return glob.glob(f'FSM_*.csv')

def bc_to_csv(business_class: str, schema: str = 1, dest_dir: str = 'datasets') -> None:
    """
    Creates a csv file in the dest_dir location of the business class data
    for a particular schema.

    business_class -- the business class to generated a file for
    schema         -- the desired schema
    dest_dir       -- the desired output location
    """
    schemas = get_schemas(business_class)
    data = pd.read_csv(get_config().get('filename_templates','bc_data_by_schema')\
        .format(business_class=business_class, version=schema), names=schemas[str(schema)], encoding='utf-8')
    
    data.to_csv(f'{dest_dir}/{business_class}_v{schema}_data.csv', index=False)

def filter_metadata(bc_metadata: dict, bc_name: str) -> BusinessClassMetadata:
    """
    Filters the business class metadata based on the specified
    columns in the columns file.

    bc_metadata -- metadata object (json) from datalake for a business class
    bc_name     -- business class name
    """
    all_cols = bc_metadata.keys()

    try:
        desired_cols = [col.replace('\n', '') for col in get_columns(get_config().get('filename_templates', 'columns_to_load').format(business_class=bc_name))]
    except FileNotFoundError as e:
        desired_cols = ''

    if not desired_cols:
        return bc_metadata

    filtered_bc_metadata = {}
    for key in set(all_cols).intersection(set(desired_cols)):
        filtered_bc_metadata[key] = bc_metadata[key]

    return filtered_bc_metadata

def resolve_col_issues(cols: list) -> List[str]:
    """
    Formats column names then resolves duplicate column name issues.
    If no duplicates are present, return formatted column names.
    Else, return formatted and resolved columns names.

    cols -- the columns of a business class for which to resolve duplicate name conflicts
    """

    formatted_columns = [format_col_name(col) for col in cols]
    duplicate_columns = list_duplicates(formatted_columns)
    
    # Determine if the list of cols doesn't have duplicates.
    if not duplicate_columns:
        return formatted_columns

    cols[:] = [f'{col}_dupe' if col in duplicate_columns else col for col in cols]
    cols[:] = [format_col_name(col) for col in cols]

    return cols

def get_columns_from_schemas(business_class: str) -> List[str]:
    """
    Returns a set of columns names based on the union of all of
    the schemas for a particular business class.

    business_class -- the business class to generate the union of schemas for
    """
    schemas = get_schemas(business_class=business_class)

    return list(set().union(*schemas.values()))

def list_duplicates(l: list) -> List:
    """
    Returns the duplicates in a list. Found on stackoverflow.com
    """
    seen = set()
    duplicates = set()
    seen_add = seen.add
    duplicates_add = duplicates.add
    for item in l:
        if item in seen:
            duplicates_add(item)
        else:
            seen_add(item)
    return list(duplicates)

def get_metadata(business_class: str) -> BusinessClassMetadata:
    """
    Returns metadata for a given business class by loading the
    metadata json file. Assumes metadata json file has been created.
    """
    with open(os.path.join(defs.ROOT_DIR, get_config().get('filename_templates', 'bc_metadata_filename').format(bc_name=business_class)), 'r') as md:
        return json.load(md)

def py_types_from_metadata(md: BusinessClassMetadata) -> BusinessClassPyType:
    """
    Returns a dictionary where the key is a column in a business
    class and the value is the columns python type.
    """
    mapping = {}
    for col, data in md.items():
        mapping[format_col_name(col)] = metadata.types.Type.map_dl_to_py_type(data)

    return mapping

def bc_to_df(filename: str, columns: list[str], types: dict = {}) -> pd.DataFrame:
    """
    Returns dataframe for a given business class.
    """
    logging.debug('---')
    logging.debug(filename)
    logging.debug(columns)
    logging.debug(types)
    logging.debug('---')
    return pd.read_csv(filename, names=columns, encoding='utf-8', dtype=types)

def bc_data_by_schema(business_class: str, bc_data_filename: str = None) -> BusinessClassDataBySchema:
    """
    Returns a dictionary of business class dataframes by schema.

    business_class   - name of business class
    bc_data_filename - string template specifying location of bc data by schema ({business_class}_v{version}.csv)
                       Defaults to `bc_data_by_schema` value in app.config if user does not supply a string template
    """
    default_filename = partial(get_config().get('filename_templates', 'bc_data_by_schema').format,
        business_class=business_class
    )

    inc_filename = partial(get_config().get('filename_templates', 'bc_data_by_schema_inc').format,
                    bc_folder=business_class,
                    active_inc_id=get_active_inc_id(),
                    bc_file=business_class
                )

    schemas = get_schemas(business_class=business_class)
    del schemas['0']

    metadata = get_metadata(business_class=business_class)
    dtypes = py_types_from_metadata(metadata)

    data = {}
    for version, schema in schemas.items():
        columns = resolve_col_issues(schema)
        f = inc_filename(version=version) if is_incremental(business_class) else default_filename(version=version)
        data[version] = bc_to_df(f, columns=columns, types=dtypes)

    return data

def bc_merged_csv(business_class: str) -> Callable[[], str]:
    """
    Iterates through all of the csv files by schema for a given business class
    then concatenates the files into one file. Returns the function that can be
    called to make the csv file.

    business_class -- the business class to generate the merged csv file for
    """

    all_columns = resolved_columns(business_class=business_class)
    data = bc_data_by_schema(business_class=business_class)
    
    def make_csv(filename: str = None) -> str:
        """
        Merges business class data from all schemas into one file and
        returns the filename.
        """
        incremental_filename = inc_data_filename(business_class=business_class)
        default_filename = get_config().get('filename_templates', 'bc_data_merged').format(business_class=business_class)
        output_filename = incremental_filename if is_incremental(business_class) else default_filename

        # Concatenate each of the individual data files, merging their schemas
        merged_data_proto = pd.concat([df for df in data.values()])[all_columns]

        # Sort the columns alphabetically for reproducibility and troubleshooting
        merged_data = merged_data_proto.reindex(sorted(merged_data_proto.columns), axis=1)

        # Output to file
        merged_data.to_csv(output_filename, index=False)

        return output_filename

    return make_csv

def is_incremental(business_class: str) -> bool:
    """
    Check if business class is configured as incremental or not.
    """
    filename = get_config().get('filename_templates', 'bc_table_config_map')
    with open(os.path.join(defs.ROOT_DIR, filename), 'r') as f:
        data = json.load(f)

        for k, v in data.items():
            if v['business_class_name'] == business_class:
                return v['incremental']

def write_db_load_payload(business_class: str) -> str:
    """
    Writes a JSON object to file that contains the payload
    required to load the csv data from the s3 bucket into the 
    business class staging table.
    """
    payload = {}
    payload['s3_bucket'] = get_config().get('aws', 's3_databrew_bucket_name')
    payload['mode'] = 'replace'
    
    filename = get_config().get('filename_templates', 'bc_table_config_map')
    with open(filename, 'r') as f:
        data = json.load(f)

        for k, v in data.items():
            if v['business_class_name'] == business_class:
                payload['target_table'] = f"SCOLumaStaging.dbo.{v['staging_table_name']}"
        
    if is_incremental(business_class):
        payload['target_file'] = s3_inc_data_filename(business_class).replace('all_schemas', 'cleansed')
    else:
        payload['target_file'] = f'output/{business_class}_cleansed.csv'

    payload_filename = get_config().get(
        'filename_templates',
        'bc_db_tbl_payload'
    ).format(business_class=business_class)
    
    with open(payload_filename, 'w') as f:
        f.write(json.dumps(payload))

    return payload_filename

def write_push_data_payload(business_class: str) -> bool:
    """
    Writes a JSON object to file that contains the payload
    required to push business class data from local machine
    to s3.
    """
    get_filename = partial(get_config().get, section='filename_templates')
    
    if is_incremental(business_class):
        all_schemas_local = inc_data_filename(business_class)
        all_schemas_s3 = f'output/{all_schemas_local}'
    else: 
        all_schemas_local = get_filename(option='bc_data_merged').format(business_class=business_class)
        all_schemas_s3 = f'output/{business_class}_all_schemas.csv'

    payload = {
        'all_schemas': all_schemas_local,
        'metadata': get_filename(option='bc_metadata_filename').format(bc_name=business_class),
        'schemas': get_filename(option='schemas').format(business_class=business_class),
        'extraction_history': get_filename(option='bc_extraction_history').format(business_class=business_class),
        'db_load_payload': get_filename(option='bc_db_tbl_payload').format(business_class=business_class),
        'all_schemas_s3': all_schemas_s3,
        'metadata_s3': f'output/{business_class}_metadata.json',
        'schemas_s3': f'output/{business_class}_schemas.json',
        'extraction_history_s3': f'output/{business_class}_extraction_history.csv',
        'db_load_payload_s3': f'output/{business_class}_db_tbl_load_payload.json'
    }

    filename = get_filename(option='bc_push_data_payload').format(business_class=business_class)
    with open(filename, 'w') as f:
        f.write(json.dumps(payload))

    return filename
    
def s3_inc_data_filename(business_class: str) -> str:
    """
    Returns the filename of the business classes incremental
    data in the s3 bucket.
    """
    s3_folder = get_config().get('aws', 's3_databrew_bucket_output_folder_name')
    s3_filename = inc_data_filename(business_class)
    
    return f'{s3_folder}{s3_filename}'

def inc_data_by_schema_filename(business_class: str):
    by_version = partial(get_config().get('filename_templates', 'bc_data_by_schema_inc').format,
                    bc_folder=business_class,
                    active_inc_id=get_active_inc_id(),
                    bc_file=business_class
                )
    
    return by_version

def data_by_schema_filename(business_class: str):
    by_version = partial(get_config().get('filename_templates', 'bc_data_by_schema').format,
        business_class=business_class
    )
    
    return by_version

def inc_data_filename(business_class: str) -> str:
    """
    Returns the filename of the incremental data for the a
    given business class and the current, active incremental id.
    """
    return get_config().get('filename_templates', 'bc_data_merged_inc')\
        .format(
            bc_folder=business_class,
            active_inc_id=get_active_inc_id(),
            business_class=business_class
        )
    
def create_inc_folders_s3(business_class: str) -> str:
    """
    Creates the incremental data folder structure in s3 for a given
    business class name and active incremental id.
    """
    bucket_put = partial(s3.put_object, bucket=get_config().get('aws', 's3_databrew_bucket_name'))

    if is_incremental(business_class=business_class):
        s3_folder = get_config().get('aws', 's3_databrew_bucket_output_folder_name')
        put_result = bucket_put(name=s3_folder + get_config().get(
            'filename_templates',
            'inc_data_active_id'
            ).format(bc_folder=business_class, active_inc_id=get_active_inc_id())
        )
        
        return put_result

def active_extraction_groups_bc() -> List[str]:
    """
    Returns a list of strings containing the names of the business classes
    in the active extraction groups.
    """
    bc = [bc for eg in get_config().get('extractions', 'active').split('\n') for bc in get_config().get('extraction_groups', eg).split('\n')]
    return bc

def is_unique_id(id):
    pattern = '[a-zA-Z0-9]{8}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{4}-[a-zA-Z0-9]{12}'
    if bool(re.match(pattern, str(id))):
        return id
    
    return 'N/A'

def apply_changes(df, col, new_col, func):
    try:
        df[new_col] = df[col].apply(func)
    except AttributeError as e:
        logging.error(e)

    return df

def extract_id_budget_fund_type(value):
    try:
        return ','.join([user_field for user_field in value.split(',') if 'IDBudgetFundType' in user_field]).split('=')[-1] if isinstance(value, str) else 'N/A'
    except Exception as e:
        logging.error(e, value, sep='\n')

def filter_transaction_amounts(val):
    if bool(re.match('[0-9]{4}-[0-9]{2}-[0-9]{2}', str(val))):
        logging.info(val, 'has date instead of amount', sep=' ')
        return 0.00

    return float(val)

def process_val(val) -> str:
    """
    Removes some characters from a string that causes problems when loading
    into a pandas dataframe using a csv file.
    """
    val = f'"{val}"'
    val = val.replace('"', '')
    val = val.replace('\n', '')

    return f'"{val}"'

def write_to_schema_file(func):
    def wrapper(self, *args, **kwargs):
        data_to_write = func(self, *args, **kwargs)      

        def write(filename, schema, records):
            logging.debug(f'Writing {len(records)} records to {filename}')
            with open(filename, 'a', encoding='utf-8') as f:
                for record in records:
                    f.write(record[0])
        
        if data_to_write:
            del data_to_write['0']
            for schema, records in data_to_write.items():
                fl_filename = self.filenames['bc_data_by_schema']\
                    .format(business_class=self.curr_bc.name, version=schema)
                il_filename = self.filenames['bc_data_by_schema_inc']\
                    .format(
                        bc_folder=f'{self.curr_bc.name}', 
                        active_inc_id=get_active_inc_id(), 
                        bc_file=f'{self.curr_bc.name}',
                        version=schema
                    )
                
                write_to = partial(write, schema = schema, records = records)

                if self.opts.incremental_load: write_to(il_filename)
                elif self.opts.full_load: write_to(fl_filename)
            
    return wrapper

def extract(func):
    def wrapper(self, *args, **kwargs):
        """
        Wraps the function that extracts data from the datalake. Main purpose
        is to identify any new schemas on the incoming data. Then, the incoming
        data are sorted into a dict based on their schema.
        """

        # Response from datalake
        datalake_response = func(self, *args, **kwargs)
        response_str = datalake_response.content.decode(encoding=datalake_response.apparent_encoding).strip()

        if datalake_response.status_code != 200:
            error_msg = f'Error sending request to datalake for {response_str}'
            logging.error(f"Something wrong with API call to datalake for url: {datalake_response.url}")
            logging.error(error_msg)
            raise Exception(error_msg)
        
        # Create generator from datalake response
        try:
            record_data = response_str.split('\n')
            self.data = [json.loads(record) for record in record_data if len(record) > 0]
        except Exception as e:
            logging.error("Bad Data")
            logging.error(e)
            logging.debug(response_str)
            raise Exception(error_msg)
        else:
            """
            This set of schemas for the current business class is used to compare
            to the schemas of the incoming records in the datalake's response.
            This set is updated any time a new schema is detected.
            """
            set_of_schemas = [set(schema) for schema in self.curr_bc.schemas.values()]
            records_by_schema = {}
            for schema in self.curr_bc.schemas.keys():
                records_by_schema[schema] = []

            for record in self.data:
                record_schema = None

                # Check if current records schemas already exists in the master list.
                record_keys = {key for key in record.keys()}
                if record_keys in set_of_schemas:
                    record_schema = f'{set_of_schemas.index(record_keys)}'
                else:
                    # New schema found
                    logging.debug('New schema found')
                    max_schema_key = max([int(key) for key in self.curr_bc.schemas.keys()])
                    newest_schema_number = f'{max_schema_key + 1}'
                    self.curr_bc.schemas[newest_schema_number] = list(record.keys())
                    records_by_schema[newest_schema_number] = []
                    set_of_schemas.append(set(record.keys()))

                row_to_write = ','.join(list(map(process_val, record.values())))
                records_by_schema[record_schema or newest_schema_number].append([row_to_write + '\n'])

            """
            Returns a dict where the key is the schema and the value are the datalake
            records assigned to that schema. This is important to do because for a given
            data object (collection of business class records), more than one schema could
            exist.
            """
            return records_by_schema

    return wrapper

def write_inc_schedule(filename, start_datetime, n=100):
    with open(filename, 'w') as f:
        for i in range(1, n + 1):
            f.write(f'{start_datetime + (86400 * i)}\n')

def split_file(source_filename: str, num_output_files: int):
    """
    Splits a single file into multiple, evenly sized files.

    source_filename - name of file to split
    num_output_files - number of files to split the source file into
    """
    file_size_gb = lambda fn: os.path.getsize(fn) / 1e9
    output_filesize_max = file_size_gb(source_filename) / num_output_files

    output_filenames = []
    source_fn_base, ext = source_filename.split('.')
    for i in range(1, num_output_files + 1):
        output_filenames.append(f"{source_fn_base}_{i}.{ext}")

    def read_file(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                yield line

    lines = read_file(source_filename)
    for f in output_filenames:
        curr_file = open(f, 'w', encoding='utf-8')
        while file_size_gb(f) < output_filesize_max:
            try:
                curr_file.write(next(lines))
            except StopIteration as e:
                curr_file.close()
                break
        else:
            curr_file.close()

    return output_filenames

def bc_config_summary(business_class: str) -> dict:
    class ETLConfig:
        def __init__(self, cfg):
            self.cfg = cfg

        def bc_stg_tbl(self, bc) -> dict:
            with open(self.cfg.get('data_resources', 'bc_table_map_filename'), 'r') as f:
                data = json.load(f)

            return data[bc] if bc in data.keys() else 'Not Found'
        
        def app_config_name(self, bc) -> dict:
            for k, v in dict(self.cfg.items('extraction_groups')).items():
                if v == bc:
                    return k
                
        def is_in_daily_etl(self, bc) -> bool:
            with open(self.cfg.get('filename_templates', 'subject_area_config'), 'r') as f:
                data = json.load(f)

            for subject_area, bcs in data.items():
                if self.app_config_name(bc) in bcs:
                    return True

            return False
        
        def addl_config(self, bc):
            with open(self.cfg.get('filename_templates', 'bc_table_config_map'), 'r') as f:
                data = json.load(f)

            if self.app_config_name(bc) in data.keys():
                return data[self.app_config_name(bc)]

            return f"Entry in {self.cfg.get('filename_templates', 'bc_table_config_map')} missing"

    cfg = ETLConfig(get_config())

    results = {
        'business_class': business_class,
        'stage_table_name': cfg.bc_stg_tbl(business_class),
        'app_config_name': cfg.app_config_name(business_class),
        'in_daily_etl': cfg.is_in_daily_etl(business_class),
        'addl_config': cfg.addl_config(business_class)
    }

    return results

def get_active_inc_id(cutoff_hour=None):
    if get_config().get('inc_extraction', 'active_inc_id_override'):
        return get_config().getint('inc_extraction', 'active_inc_id_override')

    if not cutoff_hour:
        cutoff_hour = int(get_config().get('inc_extraction', 'cutoff_hour'))

    curr = datetime.datetime.now().timestamp()
    mountain_time = curr - 21600 # Go back 6 hours to get to mountain time
    curr = datetime.datetime.fromtimestamp(mountain_time)

    y,m,d,h = (curr.year,
            curr.month,
            curr.day,
            curr.hour)

    return int((datetime.datetime(y,m,d,5,0,0).timestamp()) - (86400 if h < (cutoff_hour) else 0))

def timer(func):
    """
    Print runtime of decorated function.
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        func()
        end_time = time.perf_counter()

        logging.info(f"{end_time - start_time}")

    return wrapper

def main():
    print(f'Active inc id: {get_active_inc_id()}')
    print(f'Active inc date: {datetime.datetime.fromtimestamp(get_active_inc_id())}')
    print(f'Active extraction groups: {active_extraction_groups_bc()}')

if __name__ == '__main__':
    main()