# https://github.com/questdb/py-questdb-client

from questdb.ingress import Sender, Buffer, IngressError, TimestampNanos, TimestampMicros
import datetime
import sys

HOST = 'localhost'
PORT = 9009


def send():
    try:
        with Sender(HOST, PORT) as sender:
            # Record with provided designated timestamp (using the 'at' param)
            # Notice the designated timestamp is expected in Nanoseconds but timestamps in
            # other columns are expected in Microseconds. The API provides convenient functions
            sender.row(
                'trades',
                symbols={'name': 'client_timestamp'},
                columns={'value': 12.4, 'valid_from': TimestampMicros.from_datetime(datetime.datetime.utcnow())},
                at=TimestampNanos.from_datetime(datetime.datetime.utcnow()))
            # If no 'at' param is passed, the server will use its own timestamp
            sender.row(
                'trades',
                symbols={'name': 'server_timestamp'},
                columns={'value': 11.4})
            sender.flush()
    except IngressError as e:
        sys.stderr.write(f'Got error: {e}')


# See the docs at https://py-questdb-client.readthedocs.io/en/latest/examples.html#explicit-buffers
def send_with_buffer():
    try:
        sender = Sender(HOST, PORT)
        sender.connect()
        buffer = Buffer()
        buffer.row(
            'trades',
            symbols={'name': 'with_buffer_client_timestamp'},
            columns={'value': 12.4},
            at=TimestampNanos.from_datetime(datetime.datetime.utcnow()))
        buffer.row(
            'trades',
            symbols={'name': 'with_buffer_server_timestamp'},
            columns={'value': 11.4})
        sender.flush(buffer)
        sender.close()
    except IngressError as e:
        sys.stderr.write(f'Got error: {e}')


if __name__ == '__main__':
    send()
    # If you want to send the same rows to multiple servers, or to decouple serialization and sending,
    # you can use explicit buffers.
    send_with_buffer()
