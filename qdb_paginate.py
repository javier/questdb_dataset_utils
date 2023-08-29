import psycopg2

connection = None


def paginate(conn, query, batch_size=10):
    cursor = None
    l_bound = 0
    while True:
        u_bound = l_bound + batch_size
        try:
            cursor = conn.cursor()
            cursor.execute(f'WITH paginated_query AS ( {query} ) SELECT * FROM paginated_query LIMIT {l_bound}, {u_bound}')
            if cursor.rowcount == 0:
                break
            for row in cursor:
                print(row[0], "\n")
        except (Exception, psycopg2.Error) as error:
            print("Error while fetching data from PostgreSQL", error)
            break
        finally:
            if cursor:
                cursor.close()
        l_bound = u_bound


try:
    connection = psycopg2.connect(
        user='admin',
        password='quest',
        host='127.0.0.1',
        port='8812',
        database='qdb')

    paginate(connection, 'SELECT x FROM long_sequence(50)')

except (Exception, psycopg2.Error) as error:
    print("Error connecting to QuestDB", error)
finally:
    if connection:
        connection.close()
