import clickhouse_connect

import csv
from glob import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import sys

def connect_clickhouse(host: str = '127.0.0.1', username: str = 'default', pwd: str = None, port: int = 8123):
    try:
        client = None
        if pwd is None:
            client = clickhouse_connect.get_client(host=host, port=port, username=username)
        else:
            client = clickhouse_connect.get_client(host=host, port=port, username=username, password=pwd)
        return client
    except Exception as e:
        print(f'Had problem connecting with error {e}.')


def create_table():
    client = connect_clickhouse()
    for country in ['ES', 'UK', 'IT', 'DE', 'FR']:
        client.command(f"""
                        CREATE TABLE IF NOT EXISTS  ecommerce_sample_test_{country} (
                        ts datetime,
                        country enum('UK'=1, 'DE'=2, 'FR'=3, 'IT'=4, 'ES'=5),
                        category enum('WOMEN'=1, 'MEN'=2, 'KIDS'=3, 'HOME'=4, 'KITCHEN'=5, 'BATHROOM'=6),
                        visits UInt32,
                        unique_visitors UInt32,
                        avg_unit_price Decimal32(4),
                        sales  Decimal64(4)
                        ) ENGINE = ReplacingMergeTree
                        PRIMARY KEY(ts, country, category);
                        """
                        )


def pull_data_files(loc: str = '*.csv') -> list:
    files = glob(loc)
    files.sort()
    return files

def insert_rows(country: str, rows: list, client: object) -> None:
    t1 = datetime.now()
    dt_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    inputs = [[datetime.strptime(row[0], dt_format), row[1], row[2], row[3], row[4], row[5], row[6]] for row in rows]
    client.insert(f"ecommerce_sample_test_{country}", inputs,
                  column_names = ['ts', 'country', 'category', 'visits',
                                  'unique_visitors', 'avg_unit_price', 'sales']
                          )
    t2 = datetime.now()
    return  (t2 - t1).total_seconds()


def file_insert(file: str):
    client = connect_clickhouse()
    print(f"working on file {file}")
    country = file[-6:-4]
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
                 total_time += insert_rows(country, rows, client)
                 rows.clear()

        if rows:
            total_time += insert_rows(country, rows, client)

    print(f"finished with file {file} time was {total_time}")


CHUNK_SIZE = 100000

if __name__ == '__main__':
    t1 = datetime.now()
    files = None
    if len(sys.argv) > 1:
        files = sys.argv[1:]
    else:
        files = pull_data_files("*.csv")

    print(f'will process these files: {files}')
    create_table()
    with ProcessPoolExecutor(max_workers=8) as poolparty:
        poolparty.map(file_insert, files)
    t2 = datetime.now()
    x = t2 - t1
    print(f"time was {x}")







