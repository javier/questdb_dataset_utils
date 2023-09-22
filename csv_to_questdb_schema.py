from tableschema import Table
from dateutil.parser import parse
import json
import sys


def get_type(csv_type, representative_value):
    if csv_type in ["string"]:
        try:
            parse(representative_value)
            return "TIMESTAMP"
        except ValueError:
            if len(representative_value) > 30:
                return "STRING"
            else:
                return "SYMBOL"
    elif csv_type in ["integer"]:
        return "LONG"
    elif csv_type in ["number"]:
        return "DOUBLE"
    elif csv_type in ["boolean"]:
        return "BOOLEAN"
    elif csv_type in ["date"]:
        return "DATE"
    elif csv_type in ["datetime"]:
        return "TIMESTAMP"
    else:
        return "STRING"

def get_representative_values(table, fields):
    i = 1
    rows = []
    representative_vals = {}
    for t in table.iter():
        rows.append(t)
        for i in range(len(t)):
            if t[i] and isinstance(t[1], str) and (not fields[i]['name'] in representative_vals or len(t[1]) > len(fields[i]['name'])):
                representative_vals[fields[i]['name']] = t[i]
        if i == 100:
            break
        else:
            i+=1

    return representative_vals


def columns_from_csv(csv_path):
    table = Table(csv_path)
    schema = table.infer()
    fields = schema['fields']
    representative_values = get_representative_values(table, fields)
    columns = []
    for field_and_index in enumerate(fields):
        index = field_and_index[0]
        field = field_and_index[1]
        column = {}
        column["name"] = field["name"]
        column["type"] = get_type(field["type"], representative_values[column["name"]] )
        columns.append(column)

    return columns


def get_create_table_statement(table_name, columns):
    columns_and_types = []
    for column in columns:
        columns_and_types.append(column["name"] + " " + column["type"])

    column_text = (", \n\t").join(columns_and_types)
    statement = f'''CREATE TABLE {table_name} (\n\t{column_text})
    TIMESTAMP (<your_ts>)
    PARTITION BY DAY WAL
    '''

    return statement


def get_curl_schema(table_name, columns):
    columns_and_types = []
    for column in columns:
        columns_and_types.append( json.dumps(column))

    column_text = (", \\\n\t").join(columns_and_types)
    statement = f"""curl  -F schema='[ \ \n\t{column_text} \\
    ]' \\
    -F data=@my_file.csv \\
    'http://localhost:9000/imp?name={table_name}&partitionBy=DAY&timestamp=<YOUR_TS>'
    """

    return statement


if __name__ == '__main__':
    table_name = None
    manifest_path = None
    if len(sys.argv) !=3:
        print("""
              Parser for CSV files. It will read the first 1000 lines from CSV and will output a CREATE TABLE
              STATEMENT compatible with questdb, applying type conversions. It will also output a curl command line that
              could be used to ingest a csv using the QuestDB REST API.

              usage: python csv_to_questdb_schema table_name path_to_csv
              """)
        exit(-1)
    else:
        table_name = sys.argv[1]
        csv_path = sys.argv[2]
        columns = columns_from_csv(csv_path)
        statement = get_create_table_statement(table_name, columns)
        print(statement)
        curl_schema = get_curl_schema(table_name, columns)
        print(curl_schema)














