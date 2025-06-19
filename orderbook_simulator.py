import argparse
import numpy as np
from datetime import datetime, timedelta
from questdb.ingress import Sender, TimestampNanos
import random
import time

def simulate_orderbook_snapshot(symbol, base_price, max_levels=5):
    levels_bid = random.randint(1, max_levels)
    levels_ask = random.randint(1, max_levels)

    bid_prices = sorted([base_price - random.uniform(0, 5) for _ in range(levels_bid)], reverse=True)
    bid_sizes = [random.randint(10, 1000) for _ in range(levels_bid)]

    ask_prices = sorted([base_price + random.uniform(0, 5) for _ in range(levels_ask)])
    ask_sizes = [random.randint(10, 1000) for _ in range(levels_ask)]

    bids = np.array([[round(p, 4), s] for p, s in zip(bid_prices, bid_sizes)])
    asks = np.array([[round(p, 4), s] for p, s in zip(ask_prices, ask_sizes)])

    return bids, asks

def send_orderbook_snapshots_to_questdb(sender, symbols, freq_ms=1000, num_snapshots=None, stream_mode=False):
    start_time = datetime.utcnow()
    i = 0
    while True:
        ts = start_time + timedelta(milliseconds=i * freq_ms)
        ts_nano = int(ts.timestamp() * 1e9)

        for symbol in symbols:
            base_price = random.uniform(100, 1000)
            bids, asks = simulate_orderbook_snapshot(symbol, base_price)

            sender.row(
                'orderbook_snapshots',
                symbols={'symbol': symbol},
                columns={
                    'bids': bids,
                    'asks': asks,
                },
                at=TimestampNanos(ts_nano)
            )
        sender.flush()
        i += 1

        if not stream_mode and num_snapshots is not None and i >= num_snapshots:
            break

        time.sleep(freq_ms / 1000)

def main():
    parser = argparse.ArgumentParser(description="Simulate orderbook snapshots and send to QuestDB")
    parser.add_argument('--symbols', type=str, default='AAPL,GOOG,MSFT', help='Comma separated list of symbols')
    parser.add_argument('--num_snapshots', type=int, default=10, help='Number of snapshots to send')
    parser.add_argument('--freq_ms', type=int, default=1000, help='Frequency in milliseconds between snapshots')
    parser.add_argument('--stream_mode', action='store_true', help='Stream mode (runs infinitely)')

    args = parser.parse_args()

    if args.stream_mode and args.num_snapshots != 10:
        # User explicitly passed num_snapshots different from default with stream_mode
        raise ValueError("Cannot specify both --num_snapshots and --stream_mode")

    symbols = [s.strip().upper() for s in args.symbols.split(',') if s.strip()]

    conf = 'http::addr=localhost:9000;'

    with Sender.from_conf(conf) as sender:
        send_orderbook_snapshots_to_questdb(
            sender,
            symbols,
            freq_ms=args.freq_ms,
            num_snapshots=None if args.stream_mode else args.num_snapshots,
            stream_mode=args.stream_mode
        )

    print("Orderbook snapshots sent to QuestDB.")

if __name__ == "__main__":
    main()
