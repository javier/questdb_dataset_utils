from questdb.ingress import Sender, IngressError, TimestampNanos
import psycopg2
from psycopg2 import extras
import csv
from glob import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
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
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS  'ecommerce_sample_test' (
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
    return files


def read_file(file: str) -> list:
    with open(file) as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)
        rows = [row for row in reader]
    return rows

def chunker(lst, n):
    chunks = [lst[i * n:(i + 1) * n] for i in range((len(lst) + n - 1) // n )]
    return chunks

def insert_rows(rows: list) -> None:
    dt_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    try:
        with Sender('localhost', 9009) as sender:
            for row in rows:
                sender.row(
                    'ecommerce_sample_test',
                    symbols={'country': row[1], 'category': row[2]},
                    columns={'visits': int(row[3]), 'unique_visitors': int(row[4]),'avg_unit_price':float(row[5]), 'sales':float(row[6])},
                    at=TimestampNanos.from_datetime(datetime.strptime(row[0], dt_format))
                    )

            sender.flush()
    except IngressError as e:
        print(f'Got error: {e} {row[3]}', flush=True)
    except Exception as e:
        print(f'Got error: {e}', flush=True)



def file_insert(file: str):
    print(f"working on file {file}", flush=True)
    rows = read_file(file)
    chunks = chunker(rows, 100000)
    t1 = datetime.now()
    for chunk in chunks:
        insert_rows(chunk)
    t2 = datetime.now()
    x = t2 - t1
    print(f"finished with file {file} time was {x}", flush=True)


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







