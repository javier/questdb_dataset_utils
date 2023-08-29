import time
import psycopg2 as pg

conn_str = 'user=admin password=quest host=127.0.0.1 port=8812 dbname=qdb'
with pg.connect(conn_str) as connection:
    with connection.cursor() as cur:

        cur.execute(''' 
                    CREATE TABLE IF NOT EXISTS new_table2(
                    ts TIMESTAMP, device_code UUID, temperature DOUBLE
                     ) timestamp(ts) PARTITION BY DAY WAL;
                     ''')

        timestamp = time.time_ns() // 1000
        cur.execute('''
                        INSERT INTO new_table2
                            VALUES (%s, %s, %s);
                        ''',
                    (timestamp, 'ab632aba-be36-43e5-a4a0-4895e9cd3f0d', 79))

        connection.commit()
        time.sleep(0.5)

        cur.execute('SELECT * FROM new_table2')
        print('Selecting rows from test table using cursor.fetchall')
        records = cur.fetchall()

        print("Print each row and it's columns values")
        for row in records:
            print("y = ", row[0], row[1], row[2], "\n")

