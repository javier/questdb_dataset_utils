#!/usr/bin/env python3

import psycopg
import argparse
import sys

# Function to generate table names
def get_table_name(base_name, index):
    return f"{base_name}{index}"

# Function to create tables using SQL template
def create_tables(conn_str, base_name, amount, create_table_sql_template):
    with psycopg.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Loop to create multiple tables
            for i in range(1, amount + 1):
                table_name = get_table_name(base_name, i)
                create_table_sql = create_table_sql_template.format(table_name=table_name)
                cur.execute(create_table_sql)
                print(f"Created table {table_name}")

# Function to drop tables
def drop_tables(conn_str, base_name, amount):
    with psycopg.connect(conn_str, autocommit=True) as conn:
        with conn.cursor() as cur:
            # Loop to drop multiple tables
            for i in range(1, amount + 1):
                table_name = get_table_name(base_name, i)
                drop_table_sql = f'DROP TABLE IF EXISTS "{table_name}";'
                cur.execute(drop_table_sql)
                print(f"Dropped table {table_name}")

def main():
    parser = argparse.ArgumentParser(description='Bulk create or drop tables in QuestDB.')

    # Mode: create or drop
    parser.add_argument('--mode', type=str, choices=['create', 'drop'], default='create', help='Mode of operation: create or drop tables (default: create)')

    # Required parameters
    parser.add_argument('--table-name', type=str, required=True, help='Base name for the tables')
    parser.add_argument('--amount', type=int, required=True, help='Number of tables to create or drop')

    # Template parameters (either --template or --template-file)
    template_group = parser.add_mutually_exclusive_group(required=False)
    template_group.add_argument('--template', type=str, help='SQL template as a string, with {table_name} as placeholder')
    template_group.add_argument('--template-file', type=str, help='Path to a file containing the SQL template')

    # Database connection parameters with defaults
    parser.add_argument('--host', type=str, default='127.0.0.1', help='Database host (default: 127.0.0.1)')
    parser.add_argument('--port', type=str, default='8812', help='Database port (default: 8812)')
    parser.add_argument('--user', type=str, default='admin', help='Database user (default: admin)')
    parser.add_argument('--password', type=str, default='quest', help='Database password (default: quest)')
    parser.add_argument('--dbname', type=str, default='qdb', help='Database name (default: qdb)')

    args = parser.parse_args()

    # Build connection string
    conn_str = f'user={args.user} password={args.password} host={args.host} port={args.port} dbname={args.dbname}'

    # Validate template parameter for create mode
    if args.mode.lower() == 'create':
        if not args.template and not args.template_file:
            print('Error: You must provide either --template or --template-file when mode is "create".')
            sys.exit(1)
        if args.template:
            create_table_sql_template = args.template
        elif args.template_file:
            try:
                with open(args.template_file, 'r') as f:
                    create_table_sql_template = f.read()
            except Exception as e:
                print(f"Error reading template file: {e}")
                sys.exit(1)
    else:
        create_table_sql_template = None  # Not needed for drop mode

    # Perform the requested operation
    if args.mode.lower() == 'create':
        create_tables(conn_str, args.table_name, args.amount, create_table_sql_template)
    elif args.mode.lower() == 'drop':
        drop_tables(conn_str, args.table_name, args.amount)

if __name__ == '__main__':
    main()


# usage example
# python bulk_table_creator_and_dropper.py \
#  --mode create \
#  --table-name test_trades \
#  --amount 3 \
#  --template "CREATE TABLE IF NOT EXISTS \"{table_name}\" ( symbol SYMBOL capacity 256 CACHE, side SYMBOL capacity 256 CACHE, price DOUBLE, amount DOUBLE, timestamp TIMESTAMP ) timestamp(timestamp) PARTITION BY DAY WAL;"

