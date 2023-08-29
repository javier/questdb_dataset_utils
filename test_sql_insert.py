import psycopg2 as pg
import time

# Connect to an existing QuestDB instance

conn_str = 'user=admin password=quest host=127.0.0.1 port=8812 dbname=qdb'
with pg.connect(conn_str) as connection:
    # Open a cursor to perform database operations

    with connection.cursor() as cur:

        # Execute a command: this creates a new table

        cur.execute('''
          CREATE TABLE IF NOT EXISTS test_pg (
              ts TIMESTAMP,
              name STRING,
              value INT
          ) timestamp(ts);
          ''')

        print('Table created.')

        # Insert data into the table.

        for x in range(10):
            # Converting datetime into millisecond for QuestDB

            timestamp = time.time_ns() // 1000

            cur.execute('''
                INSERT INTO test_pg
                    VALUES (%s, %s, %s);
                ''',
                        (timestamp, 'python example', 75))

        print('Rows inserted.')

        # Query the database and obtain data as Python objects.

        cur.execute('SELECT * FROM test_pg;')
        records = cur.fetchall()
        for row in records:
            print(row)

# the connection is now closed
