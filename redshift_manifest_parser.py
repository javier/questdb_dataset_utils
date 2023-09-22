import json
import sys

def get_type(redshift_type):
    if redshift_type in ["character varying", "char", "character", "nchar", "varchar", "nvarchar", "bpchar"]:
        return "SYMBOL"
    elif redshift_type in ["text"]:
        return "STRING"
    elif redshift_type in ["smallint", "int2"]:
        return "SHORT"
    elif redshift_type in ["integer", "int","int4"]:
        return "INT"
    elif redshift_type in ["bigint", "int8"]:
        return "LONG"
    elif redshift_type in ["float4"]:
        return "FLOAT"
    elif redshift_type in ["decimal", "float8", "float"]:
        return "DOUBLE"
    elif redshift_type in ["boolean"]:
        return "BOOLEAN"
    elif redshift_type in ["date"]:
        return "DATE"
    elif redshift_type in ["timestamp", "timestamptz"]:
        return "TIMESTAMP"
    elif redshift_type in ["varbyte"]:
        return "BINARY"
    else:
        return "STRING"


def parse_manifest(manifest_path):
    json_data = json.load(open(manifest_path))
    elements = json_data["schema"]["elements"]
    columns = []
    for column in elements:
        columns.append(column["name"] + " " + get_type(column["type"]["base"]))

    return columns

def get_create_table_statement(table_name, columns):
    column_text = (", \n\t").join(columns)
    statement = f'''CREATE TABLE {table_name} (\n\t{column_text})
    TIMESTAMP (<your_ts>)
    PARTITION BY DAY WAL
    '''

    return statement


if __name__ == '__main__':
    table_name = None
    manifest_path = None
    if len(sys.argv) !=3:
        print("""
              Parser for Redshift UNLOAD MANIFEST VERBOSE files. It will take the schema and will output a CREATE TABLE
              STATEMENT compatible with questdb, applying type conversions.

              usage: python redshift_manifest_parser table_name path_to_manifest
              """)
        exit(-1)
    else:
        table_name = sys.argv[1]
        manifest_path = sys.argv[2]
        columns = parse_manifest(manifest_path)
        statement = get_create_table_statement(table_name, columns)
        print(statement)














