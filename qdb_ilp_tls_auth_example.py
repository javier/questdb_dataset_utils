# https://github.com/questdb/py-questdb-client

from questdb.ingress import Sender, IngressError, TimestampNanos, TimestampMicros
import datetime
import sys
import os

HOST = 'demo-python-client-tls-a664199e.ilp.b04c.questdb.net'
PORT = 32174


def send_with_auth():
    try:
        auth = (os.environ['QDB_KID'], os.environ['QDB_D'], os.environ['QDB_X'], os.environ['QDB_Y'])
        with Sender(HOST, PORT, auth=auth, tls=True) as sender:
            buffer = sender.new_buffer()
            buffer.row(
                'trades',
                symbols={'name': 'tls_client_timestamp'},
                columns={'value': 12.4, 'valid_from': TimestampMicros.from_datetime(datetime.datetime.utcnow())},
                at=TimestampNanos.from_datetime(datetime.datetime.utcnow()))
            sender.flush(buffer)
    except IngressError as e:
        sys.stderr.write(f'Got error: {e}')


if __name__ == '__main__':
    send_with_auth()
