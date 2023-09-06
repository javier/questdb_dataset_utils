import yfinance as yf
import pandas as pd
from questdb.ingress import Sender, IngressError
import sys
import requests
import json

def run_query(host, sql_query):
    query_params = {'query': sql_query, 'fmt' : 'json'}
    try:
        response = requests.get(host + '/exec', params=query_params)
        json_response = json.loads(response.text)
        #print(json_response)
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}')

def create_table(host, table_name):
     run_query(host,
        f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
            Ticker SYMBOL capacity 256 CACHE,
            Open DOUBLE,
            High DOUBLE,
            Low DOUBLE,
            Close DOUBLE,
            AdjClose DOUBLE,
            Volume LONG,
            Timestamp TIMESTAMP
    ) timestamp (Timestamp) PARTITION BY MONTH WAL;
    """
     )

     run_query(host,
        f"""
            TRUNCATE TABLE {table_name} (
        """
     )


def get_tickers(tickerStrings, start, end):
    df_list = list()

    for ticker in tickerStrings:
        data = yf.download(ticker, group_by="Ticker", start=start, end=end)
        data['Ticker'] = ticker  # add this column because the dataframe doesn't contain a column with the ticker
        df_list.append(data)
        # combine all dataframes into a single dataframe
        df = pd.concat(df_list)

    df.index = pd.to_datetime(df.index)
    df.reset_index(inplace=True)
    df.rename(columns={'Adj Close': 'AdjClose'}, inplace=True)

    return df

def write_table(df, table_name, host, port):
    try:
        with Sender(host, port) as sender:
            sender.dataframe(df, table_name=table_name, symbols=['Ticker'], at='Date')
    except IngressError as e:
            sys.stderr.write(f'Got error: {e}\n')

if __name__ == '__main__':
     table_name = "nasdaq_open_close"
     create_table('http://localhost:9000', table_name)
     tickerStrings = ['TSLA', 'NVDA', 'AMD', 'AVGO', 'AMZN', 'META', 'GOOGL', 'AAPL', 'MSFT']
     start = '2017-09-01'
     end = '2023-09-06'
     df = get_tickers(tickerStrings, start, end)
     write_table(df, table_name, 'localhost', 9009)
