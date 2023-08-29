# https://github.com/questdb/py-questdb-client

from questdb.ingress import Sender, IngressError, TimestampNanos, TimestampMicros
import datetime
import time
import sys
import os

HOST = 'demo-python-client-tls-a664199e.ilp.b04c.questdb.net'
PORT = 32174


def send_with_auth():
    i = 0
    try:
        auth = (os.environ['QDB_KID'], os.environ['QDB_D'], os.environ['QDB_X'], os.environ['QDB_Y'])
        with Sender(HOST, PORT, auth=auth, tls=True) as sender:
            while i < 10000:
                sender.row(
                    'trades_3',
                    symbols={'name': 'tls_client_timestamp'},
                    columns={'value': i, 'valid_from': TimestampMicros.from_datetime(datetime.datetime.utcnow())},
                    at=TimestampNanos.from_datetime(datetime.datetime.utcnow()))
                sender.flush()
                i += 1
                time.sleep(0.25)
            sender.flush()
    except IngressError as e:
        sys.stderr.write(f'Got error: {e}')


if __name__ == '__main__':
    send_with_auth()
