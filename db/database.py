import pyodbc
import logging
from config.config import get_config

class Database():    
    def __init__(self, conn_string=None, config=None):
        self._cursor = None
        self._conn = None
        self.config = config or get_config()
        self.conn_string = conn_string or self.config.get('database', 'connection_string_pw').replace("'", '')

    def connect(self):
        self._conn = pyodbc.connect(self.conn_string)
        self._cursor = self._conn.cursor()

        return self
      
    def execute(self, query, params=[]):
        try:
            self.connect()
            self._cursor.execute(query, params)
            self._cursor.commit()
        except Exception as e:
            logging.error(e)
            raise e
        finally:
            self._cursor.close()
            self._conn.close()

    def executemany(self, query, iterable):
        try:
            self.connect()
            self._cursor.fast_executemany = True
            self._cursor.executemany(query, iterable)
            self._cursor.commit()
        except Exception as e:
            logging.error(e)
            raise e
        finally:
            self._cursor.close()
            self._conn.close()

    def __str__(self):
        return "%s class representing an instance of the mssql database server." % (self.__class__.__name__)

def main():
    db = Database()
    get_results = db.execute("select top 5 * from transparency.dbo.gl_transactions")
    logging.debug(get_results)
    
if __name__ == '__main__':    
    main()