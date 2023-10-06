from questdb.ingress import Sender, IngressError, TimestampNanos
import psycopg2
from psycopg2 import extras
import csv
from glob import glob
from datetime import datetime
import sys

def connect_postgres(host: str = '127.0.0.1', user: str = 'admin', pwd: str = 'quest', port: int = 8812, dbname: str = 'qdb'):
    try:
        conn = psycopg2.connect(f'user={user} password={pwd} host={host} port={port} dbname={dbname}')
        conn.autocommit = False

        return conn
    except psycopg2.Error as e:
        print(f'Had problem connecting with error {e}.')


def get_table_meta(table_name):
    meta = { "table_name": table_name, "partition" : None, "wal": False, "dedup" : None, "upsertKeys" : [],
            "columns" : {}, "symbols": [], "designated" : None, "columns_sql" : [] }
    conn = connect_postgres()
    with conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
                    SELECT * FROM tables WHERE name = '{table_name}';
                    """
                    )
        row = cur.fetchone()
        meta["dedup"] = row.get("dedup")
        meta["designated"] = row.get("designatedTimestamp")
        meta["partition"] = row.get("partitionBy")
        meta["wal"] = row.get("walEnabled")
        meta["maxUncommittedRows"] = row.get("maxUncommittedRows")
        meta["o3MaxLag"] = row.get("o3MaxLag")


        cur.execute(f"""
                    SELECT * FROM table_columns('{table_name}');
                    """
                    )
        records = cur.fetchall()
        for row in records:
            column_name = row["column"]
            column_type = row["type"]
            meta["columns"][column_name]={"type": column_type}
            if row["upsertKey"]:
                meta["upsertKeys"].append(column_name)
            if column_type == "SYMBOL":
                meta["symbols"].append(column_name)
                if row["symbolCached"]:
                    cached_sql = "CACHE"
                else:
                    cached_sql = "NOCACHE"

                if row["indexed"]:
                    cached_sql = 'INDEX CAPACITY {row["indexBlockCapacity"]}'
                else:
                    index_sql = ""
                meta["columns_sql"].append(f'{column_name} SYMBOL CAPACITY {row["symbolCapacity"]} {cached_sql} {index_sql}')
            else:
                meta["columns_sql"].append(f"{column_name} {column_type}")

    return meta




def insert_rows(rows: list) -> None:
    t1 = datetime.now()
    try:
        dt_format = '%Y-%m-%dT%H:%M:%S.%fZ'
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
    t2 = datetime.now()
    return  (t2 - t1).total_seconds()




if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("usage: table_migrator.py table_name")
        exit()
    else:
        table_name = sys.argv[1]


    table_meta = get_table_meta(table_name)
    print(table_meta)









