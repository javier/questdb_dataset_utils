import psycopg2
from psycopg2 import extras
import csv
from glob import glob
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import sys

def connect_postgres(host: str = '127.0.0.1', user: str = 'postgres', pwd: str = 'quest', port: int = 5432):
    try:
        conn = psycopg2.connect(f'user={user} password={pwd} host={host} port={port}')
        conn.autocommit = False

        return conn
    except psycopg2.Error as e:
        print(f'Had problem connecting with error {e}.')


def create_table():
    conn = connect_postgres()
    with conn.cursor() as cur:
            cur.execute("""
                        CREATE TABLE IF NOT EXISTS  ecommerce_sample_test (
                        ts TIMESTAMPTZ,
                        country TEXT,
                        category TEXT,
                        visits INT,
                        unique_visitors INT,
                        avg_unit_price DOUBLE PRECISION NULL,
                        sales DOUBLE  PRECISION NULL,
                        UNIQUE (ts, country, category)
                        );

                        """
                        )
            cur.execute("""
                    CREATE UNIQUE INDEX IF NOT EXISTS ecommerce_sample_unique_idx ON ecommerce_sample_test(ts,country, category);
                        """)
            cur.execute("""
                    SELECT create_hypertable('ecommerce_sample_test', 'ts', if_not_exists => TRUE);
                        """)
            cur.execute("""
                    CREATE INDEX IF NOT EXISTS ecommerce_sample_idx ON ecommerce_sample_test(ts,country, category);

                        """)
    conn.commit()



def pull_data_files(loc: str = '*.csv') -> list:
    files = glob(loc)
    return files

def insert_rows(rows: list, conn: object) -> None:
    cur = conn.cursor()
    inputs = [[row[0], row[1], row[2], row[3], row[4], row[5], row[6]] for row in rows]
    extras.execute_values(cur, """
                          INSERT INTO ecommerce_sample_test VALUES %s
                          """, inputs)
    conn.commit()


def file_insert(file: str):
    conn = connect_postgres()
    print(f"working on file {file}")
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
                 total_time += insert_rows(rows, conn)
                 rows.clear()

        if rows:
            total_time += insert_rows(rows, conn)

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
    with ProcessPoolExecutor(max_workers=5) as poolparty:
        poolparty.map(file_insert, files)
    t2 = datetime.now()
    x = t2 - t1
    print(f"time was {x}")







