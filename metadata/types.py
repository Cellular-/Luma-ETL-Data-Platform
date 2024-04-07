from resources.dictdefinitions import BusinessClassMetadata
from typing import Union

class Type:
    @classmethod
    def map_props(cls, col, col_metadata: BusinessClassMetadata):
        # Parse provided info
        md_type = col_metadata['type']
        length = col_metadata['maxLength'] if 'maxLength' in col_metadata.keys() else None
        format = col_metadata['format'] if 'format' in col_metadata.keys() else None
        is_enum = True if 'enum' in col_metadata.keys() else False

        # String special cases
        if md_type == 'string':
            # String enums
            if is_enum:
                length = 120

            # String datetimes
            elif format is not None and ('date-time' in format or 'date' in format):
                pass

            # Strings without maxLength
            elif length is None:
                length = 'MAX'

            # String with an excessive maxLength
            elif length > 8000:
                length = 'MAX'

            # Override for AsyncActionRequest.ActionParameters
            elif length == 1 and col_metadata['description'] == 'From table AsyncActionRequest and column ActionParameters' :
                length = 'MAX'

            else:
                pass

        elif md_type == 'boolean':
            length = '5'

        elif md_type == 'number':
            length = '18,2'
            # length = '38,28'

        elif md_type == 'integer':
            pass

        else:
            pass

        return {
            'col_name': col,
            'type': Type.map_to_sql_type(col_metadata),
            'length': length
        }

    @classmethod
    def is_string_enum(cls, col_metadata: BusinessClassMetadata) -> bool:
        return col_metadata['type'] == 'string' and 'enum' in col_metadata.keys()

    @classmethod
    def is_numeric_enum(cls, col_metadata: BusinessClassMetadata) -> bool:
        return (col_metadata['type'] == 'integer' or col_metadata['type'] == 'number') and 'enum' in col_metadata.keys()

    @classmethod
    def is_date(cls, col_metadata: BusinessClassMetadata) -> bool:
        return col_metadata['type'] == 'string' and 'format' in col_metadata.keys()

    @classmethod
    def map_dl_to_py_type(cls, col_metadata: BusinessClassMetadata) -> Union[str, bool, int, float]:
        """
        Takes an instance of a BusinessClassMetadata and returns its python type.
        """
        # return str
        if 'string' == col_metadata['type']:
            return str

        if 'boolean' == col_metadata['type']:
            return bool
        
        if 'integer' == col_metadata['type']:
            return int
        
        if 'number' == col_metadata['type']:
            return float

    @classmethod
    def map_to_sql_type(cls, col_metadata: BusinessClassMetadata) -> str:
        if 'string' == col_metadata['type']:
            if 'format' in col_metadata.keys():
                return 'datetime2'
            else:
                return 'varchar'

        if 'boolean' == col_metadata['type']:
            return 'varchar'
        
        if 'integer' == col_metadata['type']:
            return 'int'
        
        if 'number' == col_metadata['type']:
            return 'decimal'

    @classmethod
    def get_length_key(cls, col_metadata: BusinessClassMetadata) -> str:
        if 'string' == col_metadata['type']:
            if 'format' in col_metadata.keys():
                return ''
            elif 'enum' in col_metadata.keys():
                return ''
            else:
                for key in col_metadata.keys():
                    if 'len' in key.lower():
                        return key

        if 'boolean' == col_metadata['type']:
            return ''
        
        if 'integer' == col_metadata['type']:
            return ''
        
        if 'number' == col_metadata['type']:
            return ''