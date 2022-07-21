import psycopg2 as pg
import psycopg2.extras
import json

connection = pg.connect(user="admin", password="quest", host="127.0.0.1", port="8812",  database="qdb")
cur = connection.cursor(cursor_factory = psycopg2.extras.RealDictCursor)
cur.execute("SELECT rnd_timestamp(to_timestamp('2021', 'yyyy'), to_timestamp('2022', 'yyyy'), 0) as ts, rnd_float() as price FROM long_sequence(5)")
print(json.dumps(cur.fetchall(), default=str))
connection.close()



