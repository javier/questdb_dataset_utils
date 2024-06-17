import argparse
import os
import sqlparse
import psycopg
import logging
import requests

def fetch_sql_content(file_path_or_url):
    if file_path_or_url.startswith('http://') or file_path_or_url.startswith('https://'):
        response = requests.get(file_path_or_url)
        response.raise_for_status()
        return response.text
    else:
        with open(file_path_or_url, 'r') as file:
            return file.read()

def execute_statements(sql_content, user, password, database, host, port, quiet, verbose, results_only, ignore_errors):
    # Setup connection with autocommit
    conn = psycopg.connect(
        dbname=database,
        user=user,
        password=password,
        host=host,
        port=port,
        autocommit=True
    )
    cursor = conn.cursor()

    # Parse SQL statements
    statements = sqlparse.split(sql_content)

    for statement in statements:
        if statement.strip():
            try:
                cursor.execute(statement)
                if not quiet:
                    if not results_only:
                        print(f"Executing: {statement}")
                    if cursor.description:
                        rows = cursor.fetchall()
                        if rows:
                            if verbose:
                                for row in rows:
                                    print(row)
                            else:
                                for row in rows[:10]:
                                    print(row)
                                if len(rows) > 10:
                                    print(f"... and {len(rows) - 10} more rows")
            except Exception as e:
                logging.error(f"Error executing statement: {statement}\n{e}")
                if not ignore_errors:
                    break

    cursor.close()
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Execute SQL statements on QuestDB.")
    parser.add_argument("file_or_url", help="File path or URL containing SQL statements.")
    parser.add_argument("--user", default="admin", help="Database user. Default is 'admin'.")
    parser.add_argument("--password", default="quest", help="Database password. Default is 'quest'.")
    parser.add_argument("--database", default="qdb", help="Database name. Default is 'qdb'.")
    parser.add_argument("--host", default="localhost", help="Database host. Default is 'localhost'.")
    parser.add_argument("--port", default=8812, type=int, help="Database port. Default is 8812.")
    parser.add_argument("--quiet", action="store_true", help="Suppress all output.")
    parser.add_argument("--verbose", action="store_true", help="Show detailed output without truncation.")
    parser.add_argument("--results-only", action="store_true", help="Show only results of the statements without showing the statements.")
    parser.add_argument("--ignore-errors", action="store_true", help="Continue executing next statements even if an error occurs.")

    args = parser.parse_args()

    sql_content = fetch_sql_content(args.file_or_url)

    execute_statements(
        sql_content=sql_content,
        user=args.user,
        password=args.password,
        database=args.database,
        host=args.host,
        port=args.port,
        quiet=args.quiet,
        verbose=args.verbose,
        results_only=args.results_only,
        ignore_errors=args.ignore_errors
    )

if __name__ == "__main__":
    main()
