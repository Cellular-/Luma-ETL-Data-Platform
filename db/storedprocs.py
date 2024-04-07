from .database import Database
from .queries import q
import os, logging, argparse

def sp_names():
    mssql_db._cursor.execute(q.stored_proc_name)
    return [row[0] for row in mssql_db._cursor.fetchall()]

def write_sp_to_file(sp_names, dest_dir):
    for spn in sp_names:
        mssql_db._cursor.execute(q.stored_proc_def.format(stored_proc_name=spn))

        if not (data := mssql_db._cursor.fetchone()):
            print(f'Could not find sp for : {spn}')
            continue

        with open(os.path.join(dest_dir, f'{spn}.sql'), 'w') as f:
            f.write(
                data[0]
            )

def main():
    parser = argparse.ArgumentParser(description='Process some files.')
    parser.add_argument('--dest_dir', help='Destination directory for processing files')
    args = parser.parse_args()
    dest_dir = args.dest_dir

    write_sp_to_file(sp_names(), dest_dir)

if __name__ == '__main__':
    try:
        mssql_db = Database()
        mssql_db.connect()
    except Exception as e:
        logging.error(f'Error connecting to database: {e}')
    else:
        main()