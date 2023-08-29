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
    client.command("""
                        CREATE TABLE IF NOT EXISTS  ecommerce_sample_test (
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

def insert_rows(rows: list, client: object) -> None:
    dt_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    inputs = [[datetime.strptime(row[0], dt_format), row[1], row[2], row[3], row[4], row[5], row[6]] for row in rows]
    client.insert("ecommerce_sample_test", inputs,
                  column_names = ['ts', 'country', 'category', 'visits',
                                  'unique_visitors', 'avg_unit_price', 'sales']
                          )



def file_insert(file: str):
    client = connect_clickhouse()
    print(f"working on file {file}")
    rows = read_file(file)
    chunks = chunker(rows, 100000)
    t1 = datetime.now()
    for chunk in chunks:
        insert_rows(chunk, client)
    t2 = datetime.now()
    x = t2 - t1
    print(f"finished with file {file} time was {x}")


if __name__ == '__main__':
    t1 = datetime.now()
    files = None
    if len(sys.argv) > 1:
        files = sys.argv[1:]
    else:
        files = pull_data_files("*.csv")

    print(f'will process these files: {files}')
    create_table()
    with ProcessPoolExecutor(max_workers=5) as poolparty:
        poolparty.map(file_insert, files)
    t2 = datetime.now()
    x = t2 - t1
    print(f"time was {x}")







