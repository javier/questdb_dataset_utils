from questdb.ingress import Sender, IngressError, TimestampNanos
import sys
import datetime


def example(host: str = 'localhost', port: int = 9009):
   with Sender(host, port) as sender:
        for i in range(0, 10):
            sender.row(
                'wal_trades4',
                symbols={
                    'pair': 'USDGBP',
                    'type': 'buy'},
                columns={
                    'traded_price': 0.83,
                    'limit_price': 0.84,
                    'qty': 100,
                    'new_column_demo': 100,
                    'traded_ts': datetime.datetime(
                        2022, 8, 6, 7, 35, 23, 189062,
                        tzinfo=datetime.timezone.utc)},
                at=TimestampNanos.now())
            sender.flush()

if __name__ == '__main__':
    example()
