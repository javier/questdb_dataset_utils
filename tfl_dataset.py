import requests
import socket
import sys
import datetime
import re
import time

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


def send_utf8(msg):
    print(msg)
    sock.sendall(msg.encode())


def parse_timestamp(timestamp):
    dt_format = ''
    if '.' in timestamp:
        dt_format = '%Y-%m-%dT%H:%M:%S.%fZ'
    else:
        dt_format = '%Y-%m-%dT%H:%M:%SZ'
    try:
        return int(datetime.datetime.strptime(timestamp, dt_format).timestamp() * 1000000000)
    except ValueError as e:
        sys.stderr.write(f'Error parsing date: {str} - {e}')
        return ''


def safe_escape(literal):
    if str is None:
        return None
    else:
        return re.sub(r"\W", "_", literal) # re.escape(str)


def prepare_msg(prediction):
    vehicleId = prediction.get('vehicleId')
    naptanId = prediction.get('naptanId')
    stationName = safe_escape(prediction.get('stationName'))
    lineId = safe_escape(prediction.get('lineId'))
    direction = safe_escape(prediction.get('direction'))
    bearing = prediction.get('bearing')
    destinationNaptanId = prediction.get('destinationNaptanId')
    destinationName = safe_escape(prediction.get('destinationName'))
    timestamp = parse_timestamp(prediction['timestamp'])
    timeToStation = prediction.get('timeToStation','')
    currentLocation = safe_escape(prediction.get('currentLocation'))
    towards = safe_escape(prediction.get('towards'))
    expectedArrival = parse_timestamp(prediction.get('expectedArrival'))
    modeName = safe_escape(prediction['modeName'])

    ilp_line = f'tfl_predictions,vehicleId={vehicleId},naptanId={naptanId},stationName={stationName},lineId={lineId},direction={direction},bearing={bearing},destinationNaptanId={destinationNaptanId},destinationName={destinationName},currentLocation={currentLocation},towards={towards},modeName={modeName} expectedArrival={expectedArrival},timeToStation={timeToStation} {timestamp}\n'

    return re.sub(r"([,]\w+=)(?=[,|\s])|[\s](\w+=)(?=[,|\s])", "", ilp_line)


def ingest_transport_mode(mode, limit=200000):
    resp = requests.get(f'https://api.tfl.gov.uk/Mode/{mode}/Arrivals?count={limit}').json()
    for prediction in resp:
        send_utf8(prepare_msg(prediction))


if __name__ == '__main__':
    try:
        sock.connect(('localhost', 9009))
        while True:
            ingest_transport_mode('bus')
            time.sleep(60)
    except socket.error as e:
        sys.stderr.write(f'Got error: {e}')
    sock.close()

