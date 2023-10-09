from questdb.ingress import Sender, IngressError, TimestampNanos
import psycopg2
from psycopg2 import extras
import csv
from glob import glob
from datetime import datetime
from textwrap import dedent
import sys
import argparse

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
    parser = argparse.ArgumentParser(description='Migrate table across servers.', formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--table-name", required=True, help='Name of the table to migrate.')
    parser.add_argument("--destination-host", required=True, help='IP or Name of destination host (for pg-wire protocol).')
    parser.add_argument("--destination-ilp-host", required=True, help='IP or Name of destination host (for ILP protocol).')

    parser.add_argument("--origin-host", default='localhost', help='IP or Name of origin host (for pg-wire protocol).')
    parser.add_argument("--origin-port", default='8812', help='Port of origin host (for pg-wire protocol).')
    parser.add_argument("--origin-user", default='admin', help='User of origin host (for pg-wire protocol).')
    parser.add_argument("--origin-password", default='quest', help='Password of the origin-user (for pg-wire protocol).')
    parser.add_argument("--origin-database", default='qdb',  help='Database in the origin host.')

    parser.add_argument("--destination-port", default='8812', help='Port of destination host (for pg-wire protocol).')
    parser.add_argument("--destination-user", default='admin', help='User of destination host (for pg-wire protocol).')
    parser.add_argument("--destination-password", default='quest', help='Password of the destination-user (for pg-wire protocol).')
    parser.add_argument("--destination-database", default='qdb', help='Database in the destination host.')
    parser.add_argument("--destination-ilp-port",  default='9009', help='Port of destination host (for ILP protocol).')
    parser.add_argument("--ilp-auth-kid", required=False, help='KID parameter for ILP authentication.')
    parser.add_argument("--ilp-auth-d", required=False, help='D parameter for ILP authentication.')
    parser.add_argument("--ilp-auth-x", required=False, help='X parameter for ILP authentication.')
    parser.add_argument("--ilp-auth-y", required=False, help='Y parameter for ILP authentication.')
    parser.add_argument("--use-tls-over-ilp", default=False, action='store_true', help='Use TLS with ILP.')


    args = parser.parse_args()

    table_name = args.table_name
    origin_host = args.origin_host
    origin_port = args.origin_port
    origin_user = args.origin_user
    origin_password = args.origin_password
    origin_database = args.origin_database
    destination_host = args.destination_host
    destination_port = args.destination_port
    destination_user = args.destination_user
    destination_password = args.destination_password
    destination_database = args.destination_database
    destination_ilp_host = args.destination_ilp_host
    destination_ilp_port = args.destination_ilp_port
    tls = args.use_tls_over_ilp

    if args.ilp_auth_kid and args.ilp_auth_d and args.ilp_auth_x and args.ilp_auth_y:
        auth = (args.ilp_auth_kid, args.ilp_auth_d, args.ilp_auth_x, args.ilp_auth_y)
    else:
        auth = None

    if origin_host == destination_host and origin_port == destination_port:
        print("Origin and destination hosts must be different.")
        exit()


    origin_conn = connect_postgres(origin_host, origin_port, origin_user, origin_password, origin_database)
    table_meta = get_table_meta(origin_conn, table_name)
    sql = get_create_statement(origin_conn, table_meta)

    destination_conn = connect_postgres(destination_host, destination_port, destination_user, destination_password, destination_database)
    create_dest_table(destination_conn, sql)
    print(f'Created table with schema: \n {sql}')

    insert_rows(origin_conn, table_meta, destination_ilp_host, destination_ilp_port, tls, auth)

    origin_conn.close()
    destination_conn.close()











