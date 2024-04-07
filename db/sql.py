from metadata.types import Type

class DDL:
    def __init__(self):
        pass

    @classmethod
    def create_table(cls, database, table_name, bc_metadata):
        START = 'CREATE TABLE [{database}].[dbo].[{table_name}] ('.format(database=database, table_name=table_name)
        END = f'[id] int IDENTITY(1,1) NOT NULL) ON [PRIMARY]'
        
        cols_ddl = []
        for col in bc_metadata.keys():
            col_ddl = DDL.column(**Type.map_props(col.replace('.', '_'), bc_metadata[col]))
            cols_ddl.append(col_ddl)

        cols_ddl = ',\n'.join(cols_ddl)

        return f"""
        {START}
        {cols_ddl},
        {END}"""

    @classmethod
    def delete_table(cls, database, table_name):
        QUERY = 'DROP TABLE IF EXISTS [{database}].[dbo].[{table_name}];'.format(database=database, table_name=table_name)

        return f"""
        {QUERY}
        """

    def column(col_name, type, length='', default_value='NULL'):
        length = f'({length})' if length else ''
        return f'[{col_name}] [{type}] {length} {default_value}'
    
    @classmethod
    def row_count(cls, database, table_name):
        QUERY = 'SELECT COUNT(*) FROM {database}.dbo.{table_name}'

        return f"""
        {QUERY}
        """