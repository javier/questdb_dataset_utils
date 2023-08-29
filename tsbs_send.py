from questdb.ingress import Sender, IngressError, TimestampNanos
import os
import sys
import random
import time

HOST = 'localhost'
PORT = 9009
DEVICE_TYPES = ["blue", "red", "green", "yellow"]
ITER = 10000
BATCH = 5000
DELAY = 0.5
MIN_LAT = 19.50139
MAX_LAT = 64.85694
MIN_LON = -161.75583
MAX_LON = -68.01197


def send(host: str = 'javier-demo-.ilp.b1t9.questdb.com', port: int = 32064):
    try:
        # auth = (os.environ['QDB_KID'], os.environ['QDB_D'], os.environ['QDB_X'], os.environ['QDB_Y'])
        # with Sender(HOST, PORT, auth=auth, tls=True) as sender:
        # with Sender(HOST, PORT) as sender:
        auth = (
            "admin",  # kid
            "XXXXX-_SavSqsBOym4_emQUynEE4s6BnJY",  # d
            "XXXXXX",  # x
            "XXXXXX-KrqH3xsCbNi_1qs")  # y
        with Sender(host, port, auth=auth, tls=True) as sender:
            for it in range(ITER):
                for i in range(BATCH):
                    sender.row(
                        'ilp_test',
                        symbols={'device_type': random.choice(DEVICE_TYPES)},
                        columns={
                                    'duration_ms': random.randint(0, 4000),
                                    "lat": random.uniform(MIN_LAT, MAX_LAT),
                                    "lon": random.uniform(MIN_LON, MAX_LON),
                                    "measure1": random.randint(-2147483648, 2147483647),
                                    "measure2": random.randint(-2147483648, 2147483647),
                                    "speed": random.randint(0, 100)
                        },
                        at=TimestampNanos.now())
                sys.stdout.write(f'sent : {BATCH} rows\n')
                sender.flush()
                time.sleep(DELAY)
    except IngressError as e:
        sys.stderr.write(f'Got error: {e}')


if __name__ == '__main__':
    send()

