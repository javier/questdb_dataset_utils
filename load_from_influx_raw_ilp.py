import socket
import sys

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

def send_utf8(msg):
    print(msg)
    sock.sendall(msg.encode())

if __name__ == '__main__':
    try:
        sock.connect(('localhost', 9009))
        with open("ilp_influx_export.ilp") as infile:
            for line in infile:
                print(line)
                send_utf8(line)
    except socket.error as e:
        sys.stderr.write(f'Got error: {e}')
    sock.close()

