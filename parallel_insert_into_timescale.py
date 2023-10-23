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
        for country in ['ES', 'UK', 'IT', 'DE', 'FR']:
            cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS  ecommerce_sample_test_{country} (
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
            cur.execute(f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS ecommerce_sample_{country}_unique_idx ON ecommerce_sample_test_{country}(ts,country, category);
                        """)
            cur.execute(f"""
                    SELECT create_hypertable('ecommerce_sample_test_{country}', 'ts', if_not_exists => TRUE);
                        """)
            cur.execute(f"""
                    CREATE INDEX IF NOT EXISTS ecommerce_sample_{country}_idx ON ecommerce_sample_test_{country}(ts,country, category);

                        """)
    conn.commit()



def pull_data_files(loc: str = '*.csv') -> list:
    files = glob(loc)
    return files


def insert_rows(country: str, rows: list, conn: object) -> None:
    cur = conn.cursor()
    t1 = datetime.now()
    inputs = [[row[0], row[1], row[2], row[3], row[4], row[5], row[6]] for row in rows]
    extras.execute_values(cur, f"""
                          INSERT INTO ecommerce_sample_test_{country} VALUES %s
                          ON CONFLICT(ts, country, category) DO UPDATE
                            SET visits = excluded.visits,
                          unique_visitors = excluded.unique_visitors,
                          avg_unit_price = excluded.avg_unit_price,
                          sales = excluded.sales;
                          """, inputs)
    conn.commit()
    t2 = datetime.now()
    return  (t2 - t1).total_seconds()

def file_insert(file: str):
    conn = connect_postgres()
    print(f"working on file {file}")
    total_time = 0
    country = file[-6:-4]
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
                 total_time += insert_rows(country, rows, conn)
                 rows.clear()

        if rows:
            total_time += insert_rows(country, rows, conn)

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







