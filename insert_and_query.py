from questdb.ingress import Sender, IngressError
import sys
import datetime
import psycopg2


def example(cursor, host: str = 'javier-demo-d9079585.ilp.c7at.questdb.com', port: int = 32081):
    try:
        # See: https://questdb.io/docs/reference/api/ilp/authenticate
        auth = (
            "admin",  # kid
            "-PPmZEaEZ4Q7lVVwItwFIV0gZ2zSkPsBkm8qYRzMG0U",  # d
            "P09DKH1VzR1gg92S00olnj-Vj2VmXvTgCt1XZG15VcY",  # x
            "hfPJqFge63mOFLtYySURG_r4B6M6kDvDfOqOxZo6pyA")  # y
        with Sender(host, port, auth=auth, tls=True) as sender:
            sender.row(
                'test_t',
                symbols={
                    'pair': 'USDGBP',
                    'type': 'buy'},
                columns={
                    'traded_price': 0.83,
                    'limit_price': 0.84,
                    'qty': 100,
                    'traded_ts': datetime.datetime(
                        2022, 8, 6, 7, 35, 23, 189062,
                        tzinfo=datetime.timezone.utc)},
                at=datetime.datetime.utcnow())

            sender.flush()

    except IngressError as e:
        sys.stderr.write(f'Got error: {e}\n')

    postgreSQL_select_Query = 'SELECT * FROM test_t;'
    cursor.execute(postgreSQL_select_Query)
    print('Selecting rows from test table using cursor.fetchall')
    mobile_records = cursor.fetchall()

    print("Print each row and it's columns values")
    for row in mobile_records:
        print("y = ", row[0], row[1], row[2], row[3], row[4], row[5], row[6], "\n")

if __name__ == '__main__':
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(
            user='admin',
            password='SYyHAb6j14iw8Nia',
            host='javier-demo-d9079585.psql.c7at.questdb.com',
            port='31491',
            database='qdb')
        cursor = connection.cursor()
        example(cursor)
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        print("PostgreSQL connection is closed")
