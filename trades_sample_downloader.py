import requests
import csv
import os
import json

def upload_table(host, from_file, table_name):
    print(f'Uploading {from_file} into {table_name}')
    params = {'name': table_name, 'overwrite': 'false', }
    files = {'data': open(from_file, 'rb'),}
    try:
        r = requests.post(host + '/imp', params=params, files=files)
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}')

def run_query(host, sql_query):
    query_params = {'query': sql_query, 'fmt' : 'json'}
    try:
        response = requests.get(host + '/exec', params=query_params)
        json_response = json.loads(response.text)
        #print(json_response)
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}')

def download_query(from_host, file_name, query):
    with open(file_name, "w") as outfile:
        writer = csv.writer(outfile)
        last_line = None
        page = 0
        while last_line != 0:
            row_from = page * 50000
            row_to = (page + 1) * 50000
            resp = requests.get(from_host + '/exp', {'query': query, 'limit': f"{row_from},{row_to}"})
            decoded_content = resp.content.decode('utf-8')
            csv_reader = csv.reader(decoded_content.splitlines(), delimiter=',')
            headers = next(csv_reader, None)
            if page == 0:
                writer.writerow(headers)
            last_line = 0
            for row in csv_reader:
                writer.writerow(row)
                last_line = csv_reader.line_num
            #print(last_line)
            page += 1


if __name__ == '__main__':
    file_name = "btc_trades.csv"
    host = 'http://localhost:9000'
    from_host = 'https://demo.questdb.io'

    download_query(from_host,
                   file_name,
                   """
                   select * from trades where timestamp  between '2023-09-05T14:00:00.000' AND '2023-09-05T23:59:59.999'
                   and symbol = 'BTC-USD';
                   """
                   )

    run_query(host, """
                CREATE TABLE IF NOT EXISTS 'trades' (
                    symbol SYMBOL capacity 256 CACHE,
                    side SYMBOL capacity 256 CACHE,
                    price DOUBLE,
                    amount DOUBLE,
                    timestamp TIMESTAMP
                    ) timestamp (timestamp) PARTITION BY DAY WAL;
              """)

    run_query(host, """
                TRUNCATE TABLE 'trades'
              """)

    upload_table(host, file_name, 'trades')
    os.remove(file_name)
