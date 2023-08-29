# https://github.com/questdb/py-questdb-client

from questdb.ingress import Sender, Buffer, IngressError, TimestampNanos, TimestampMicros
import time
import sys
import os

HOST = 'localhost'
PORT = 9009
USE_AUTH = False
AUTH = (os.environ['QDB_KID'], os.environ['QDB_D'], os.environ['QDB_X'], os.environ['QDB_Y'])


def connect():
    try:
        if USE_AUTH:
            sender = Sender(HOST, PORT, auth=AUTH, tls=True, auto_flush=False)
        else:
            sender = Sender(HOST, PORT, auto_flush=False)
        sender.connect()
        return sender
    except IngressError as e:
        sys.stderr.write(f'Got error: {e}')
        return None


# See the docs at https://py-questdb-client.readthedocs.io/en/latest/examples.html#explicit-buffers
def send_with_backoff():
    sender = connect()
    if sender is None:
        exit(1)

    rounds = 10000
    i = 0
    skip = 1
    buffer = Buffer()
    skip_i = 0
    skip_goal = None
    while i < rounds:
        buffer.row(
            'trades_test_3',
            symbols={'name': 'with_buffer_server_timestamp'},
            columns={'value': i})
        if skip_goal is None:
            try:
                print(f"sending")
                sender.flush(buffer, False)
                buffer.clear()
            except IngressError as e:
                sys.stderr.write(f'Got error: {e}')
                skip_goal = 5
        else:
            skip_i += 1
            if skip_i == skip_goal:
                print(f"Trying to reconnect")
                sender = connect()
                if sender is not None:
                    print(f"connection recovered")
                    sender.flush(buffer, True)
                    skip_goal = None
                    skip_i = 0
                else:
                    skip_goal = 5
                    skip_i = 0
                    print(f"goal is {skip_goal}")

        i += 1
        time.sleep(0.5)
        print(f"i is {i}")
    sender.close()


if __name__ == '__main__':
    send_with_backoff()
