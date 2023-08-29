import psycopg as pg
import time

conn_str = 'user=admin password=quest host=127.0.0.1 port=8812 dbname=qdb'
with pg.connect(conn_str) as connection:
    with connection.cursor() as cur:
        cur.execute('SELECT * FROM ilp_test limit 5;')
        records = cur.fetchall()
        for row in records:
            print(row)


