from questdb.ingress import Sender, IngressError, TimestampNanos
import psycopg2
from psycopg2 import extras
import csv
from glob import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import sys

def connect_postgres(host: str = '127.0.0.1', user: str = 'admin', pwd: str = 'quest', port: int = 8812, dbname: str = 'qdb'):
    try:
        conn = psycopg2.connect(f'user={user} password={pwd} host={host} port={port} dbname={dbname}')
        conn.autocommit = False

        return conn
    except psycopg2.Error as e:
        print(f'Had problem connecting with error {e}.')


def create_table():
    conn = connect_postgres()
    with conn.cursor() as cur:
            for country in ['ES', 'UK', 'IT', 'DE', 'FR']:
                cur.execute(f"""
                            CREATE TABLE IF NOT EXISTS  'ecommerce_sample_test_{country}' (
                            ts TIMESTAMP,
                            country SYMBOL capacity 256 CACHE,
                            category SYMBOL capacity 256 CACHE,
                            visits LONG,
                            unique_visitors LONG,
                            avg_unit_price DOUBLE,
                            sales DOUBLE
                            ) timestamp (ts) PARTITION BY DAY WAL DEDUP UPSERT KEYS(ts,country,category);

                            """
                            )
    conn.commit()


def pull_data_files(loc: str = '*.csv') -> list:
    files = glob(loc)
    files.sort()
    return files

global EPOCHS
EPOCHS = {'ES': mp.Value('l', 1657888365426838), 'FR': mp.Value('l', 1657888365426838), 'IT': mp.Value('l', 1657888365426838), 'UK': mp.Value('l', 1657888365426838), 'DE': mp.Value('l', 1657888365426838)}

def insert_rows(country, sender, rows: list) -> None:
    t1 = datetime.now()

    dt_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    for row in rows:
        sender.row(
            f'ecommerce_sample_test_{country}',
            symbols={'country': row[1], 'category': row[2]},
            columns={'visits': int(row[3]), 'unique_visitors': int(row[4]),'avg_unit_price':float(row[5]), 'sales':float(row[6])},
            #at=TimestampNanos.from_datetime(datetime.strptime(row[0], dt_format))
            at=TimestampNanos(EPOCHS[country].value)
            )
            with EPOCHS[country].get_lock():
                EPOCHS[country].value += 60000000000

    t2 = datetime.now()
    return  (t2 - t1).total_seconds()

def file_insert(file: str):
    print(f"working on file {file}")
    country = file[-6:-4]
    try:
        with Sender('localhost', 9009) as sender:
            total_time = 0
            with open(file) as csvfile:
                reader = csv.reader(csvfile)
                next(reader, None)
                rows = []
                i = 0
                for row in reader:
                    rows.append(row)
                    i += 1
                    if i == CHUNK_SIZE:
                        i = 0
                        total_time += insert_rows(country, sender, rows)
                        rows.clear()

                if rows:
                    total_time += insert_rows(country, sender, rows)
            sender.flush()
    except IngressError as e:
        print(f'Got error: {e} {row[3]}', flush=True)
    except Exception as e:
        print(f'Got error: {e}', flush=True)

    print(f"finished with file {file} time was {total_time}")

CHUNK_SIZE = 100000


if __name__ == '__main__':
    t1 = datetime.now()
    files = None
    if len(sys.argv) > 1:
        files = sys.argv[1:]
    else:
        files = pull_data_files("*.csv")

    print(f'will process these files: {files}\n')
    create_table()
    with ProcessPoolExecutor(max_workers=5) as poolparty:
       poolparty.map(file_insert, files)
    t2 = datetime.now()
    x = t2 - t1
    print(f"time was {x}")







