import yfinance as yf
import pandas as pd
from questdb.ingress import Sender, IngressError
import sys

def get_tickers(tickerStrings, start, end):
    df_list = list()

    for ticker in tickerStrings:
        data = yf.download(ticker, group_by="Ticker", start=start, end=end)
        data['ticker'] = ticker  # add this column because the dataframe doesn't contain a column with the ticker
        df_list.append(data)
        # combine all dataframes into a single dataframe
        df = pd.concat(df_list)

    df.index = pd.to_datetime(df.index)
    df.reset_index(inplace=True)

    return df

def write_table(df, table_name, host, port):
    try:
        with Sender(host, port) as sender:
            sender.dataframe(df, table_name=table_name, symbols=['ticker'], at='Date')
    except IngressError as e:
            sys.stderr.write(f'Got error: {e}\n')

if __name__ == '__main__':
     tickerStrings = ['TSLA', 'NVDA', 'AMD', 'AVGO', 'AMZN', 'META', 'GOOGL', 'AAPL', 'MSFT']
     start = '2017-09-01'
     end = '2023-08-31'
     df = get_tickers(tickerStrings, start, end)
     write_table(df, 'nasdaq', 'localhost', 9009)
