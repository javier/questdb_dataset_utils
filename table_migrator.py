from questdb.ingress import Sender, IngressError, TimestampNanos
import psycopg2
from psycopg2 import extras
import csv
from glob import glob
from datetime import datetime
from textwrap import dedent
import sys

def connect_postgres(host: str = '127.0.0.1', port: int = 8812,  user: str = 'admin', pwd: str = 'quest', dbname: str = 'qdb'):
    try:
        conn = psycopg2.connect(f'user={user} password={pwd} host={host} port={port} dbname={dbname}')
        conn.autocommit = False

        return conn
    except psycopg2.Error as e:
        print(f'Had problem connecting with error {e}.')


def get_table_meta(conn, table_name):
    meta = { "table_name": table_name, "partition" : None, "wal": False, "dedup" : None, "upsertKeys" : [],
            "columns" : {}, "symbols": [], "designated" : None, "columns_sql" : [], "with" : [] }

    with conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor) as cur:
        cur.execute(f"""
                    SELECT * FROM tables WHERE name = '{table_name}';
                    """
                    )
        row = cur.fetchone()
        meta["dedup"] = row.get("dedup")
        meta["designated"] = row.get("designatedTimestamp")
        meta["partition"] = row.get("partitionBy")
        if meta["partition"] == "NONE":
            meta["partition"] = None
        meta["wal"] = row.get("walEnabled")
        if row.get("maxUncommittedRows"):
            meta["with"].append(f' maxUncommittedRows={row["maxUncommittedRows"]} ')

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

def get_create_statement(conn, table_meta):

    columns_sql = ",\n\t".join(table_meta["columns_sql"])
    if table_meta["designated"]:
        designated_sql = f' TIMESTAMP({table_meta["designated"]}) '
    else:
        designated_sql = ""
    if table_meta["partition"]:
        partition_sql = f' PARTITION BY {table_meta["partition"]} '
    else:
        partition_sql = ""
    if table_meta["wal"]:
        wal_sql = f' WAL '
    else:
        wal_sql = ""
    if table_meta["dedup"]:
        upsert_sql = ", ".join(table_meta["upsertKeys"])
        dedup_sql = f" DEDUP UPSERT KEYS({upsert_sql}) "
    else:
        dedup_sql = ""
    if table_meta["with"]:
        with_sql =  f' WITH {", ".join(table_meta["with"])}'
    else:
        with_sql = ""


    sql = f"""\
    CREATE TABLE IF NOT EXISTS {table_meta['table_name']} (
    \t{columns_sql}
    ) {designated_sql} {partition_sql} {wal_sql} {with_sql} {dedup_sql};
    """
    return sql

def create_dest_table(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)

def prepare_columns(row, col_list):
    cols={}
    for col in col_list:
        if row[col]:
            cols[col] = row[col]

    return cols

def insert_rows(origin_conn, table_meta, dest_ilp_host, dest_ilp_port, tls, auth):
    table_name = table_meta['table_name']
    offset = 0
    chunk_size = 10000
    limit = chunk_size
    dt_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    symbol_list = table_meta['symbols']
    designated_timestamp = table_meta['designated']
    column_list = table_meta['columns'].keys() - (symbol_list + [designated_timestamp])

    with Sender(dest_ilp_host, dest_ilp_port, auth=auth, tls=tls) as sender:
        row_number = 0
        try:
            while True:
                with origin_conn.cursor(cursor_factory = psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(f"""
                                SELECT * FROM {table_name}  LIMIT {offset}, {limit};
                                """
                                )
                    if cur.rowcount == 0:
                        print(f"Ingested {row_number} rows")
                        break
                    else:
                        offset += chunk_size
                        limit += chunk_size

                    records = cur.fetchall()
                    for row in records:
                        row_number += 1
                        symbols = prepare_columns(row, symbol_list)
                        columns = prepare_columns(row,  column_list)
                        sender.row(
                             table_name,
                             symbols=symbols,
                             columns=columns,
                             at=TimestampNanos.from_datetime(row[designated_timestamp])
                            )

            sender.flush()
        except IngressError as e:
            print(f'Got error: {e} {row}', flush=True)
        except Exception as e:
            print(f'Got error: {e}', flush=True)

    return




if __name__ == '__main__':
    if len(sys.argv) != 14 and len(sys.argv) != 19:
        print("usage: table_migrator.py table_name origin_pg_host origin_pg_port origin_pg_user origin_pg_password origin_database destination_pg_host destination_pg_port destination_pg_user destination_pg_password destination_database destination_ilp_host destination_ilp_port tls_flag auth_kid auth_d auth_x auth_y")
        exit()

    table_name = sys.argv[1]
    origin_host = sys.argv[2]
    origin_port = sys.argv[3]
    origin_user = sys.argv[4]
    origin_password = sys.argv[5]
    origin_database = sys.argv[6]
    destination_host = sys.argv[7]
    destination_port = sys.argv[8]
    destination_user = sys.argv[9]
    destination_password = sys.argv[10]
    destination_database = sys.argv[11]
    destination_ilp_host = sys.argv[12]
    destination_ilp_port = sys.argv[13]

    if len(sys.argv) == 19:
        tls = sys.argv[14].lower() in ('true', '1', 't')
        auth = (sys.argv[15], sys.argv[16], sys.argv[17], sys.argv[18])
    else:
        tls = False
        auth = None



    origin_conn = connect_postgres(origin_host, origin_port, origin_user, origin_password, origin_database)
    table_meta = get_table_meta(origin_conn, table_name)
    sql = get_create_statement(origin_conn, table_meta)

    destination_conn = connect_postgres(destination_host, destination_port, destination_user, destination_password, destination_database)
    create_dest_table(destination_conn, sql)
    print(f'Created table with schema: \n {sql}')

    insert_rows(origin_conn, table_meta, destination_ilp_host, destination_ilp_port, tls, auth)

    origin_conn.close()
    destination_conn.close()











