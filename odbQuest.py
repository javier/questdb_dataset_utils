# configuring ODBC https://solutions.posit.co/connections/db/best-practices/drivers/
# using ODBC from python https://github.com/mkleehammer/pyodbc/wiki/Getting-started
import pyodbc

con = pyodbc.connect(
  driver = 'PostgreSQL Driver',
  database = 'qdb',
  server = 'localhost',
  port = 8812,
  uid = 'admin',
  pwd = 'quest'
)

cursor = con.cursor()
cursor.execute("select * from ilp_test limit 5")

while True:
    row = cursor.fetchone()
    if not row:
        break
    print(row)
