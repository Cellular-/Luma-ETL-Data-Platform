from .database import Database
from .queries import q
from config.config import get_config, setup_logging
from utilities.aws.sns import AWSSNSService
import logging, textwrap, os

def get_active_queries(mssql_db):
    mssql_db._cursor.execute(q.active_queries)
    columns = [column[0] for column in mssql_db._cursor.description]

    results = []
    for row in mssql_db._cursor.fetchall():
        results.append(dict(zip(columns, row)))
    return results

def parse_active_queries(active_queries):
    results = []
    for row in active_queries:
        elapsed_time = row['TimeElapsed']
        results.append(
            {
                'spid': row['SPID'],
                'query_id': row['query_id'],
                'host': row['Host'],
                'elapsed_hours': elapsed_time.hour,
                'elapsed_mins': elapsed_time.minute
            }
        )

    return results

if __name__ == '__main__':
    os.chdir(__package__)

    # Create a logger with the name based on __spec__.name
    log_name = f'{__spec__.name}.log'
    logger = logging.getLogger(log_name)

    # Set the logging level (you can change this to suit your needs)
    logger.setLevel(logging.DEBUG)

    # Create a file handler to write logs to a file
    file_handler = logging.FileHandler(log_name)

    # Create a formatter to format log messages
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Set the formatter for the file handler
    file_handler.setFormatter(formatter)

    # Add the file handler to the logger
    logger.addHandler(file_handler)
    try:
        mssql_db = Database(conn_string=get_config().get('database', 'connection_string_pw_admin').replace("'", ''))
        mssql_db.connect()
        active_queries = parse_active_queries(get_active_queries(mssql_db=mssql_db))
        aws_sns = AWSSNSService(
            get_config()
        )

        def long_running(active_query, threshold):
            hours = active_query['elapsed_hours']
            mins = active_query['elapsed_mins']

            return (hours * 60 + mins) >  threshold

        threshold = 25
        active_queries_long = list(filter(lambda a: long_running(a, threshold), active_queries))
        active_queries_long_msg = []
        for query in active_queries_long:
            msg = ','.join([str(value) for value in query.values()])
            active_queries_long_msg.append(msg)
        active_queries_long_msg = '\n'.join(active_queries_long_msg)
        
        if active_queries_long:
            logger.info('Long running queries found')
            message = textwrap.dedent(f"""
            The following queries have been running for more than {threshold} minutes
            {','.join(k for k in active_queries_long[0].keys())}
            {active_queries_long_msg}
            """)

            aws_sns.publish_message(
                'Luma DW Long Running Queries',
                message=message
            )
            logger.info(f'Published this message {message}')
        else:
            logger.info(f'No long running queries found')
    except Exception as e:
        logger.error(f'Error connecting to database: {e}')
