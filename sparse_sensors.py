import psycopg2
from psycopg2 import sql
import random
import time
from datetime import datetime, timedelta
import argparse
import csv
from questdb.ingress import Sender, TimestampNanos
import sys

# Function to generate vehicle IDs
def generate_vehicle_ids(num_vehicles):
    vehicle_ids = []
    prefix_chars = [chr(i) for i in range(65, 91)]  # A-Z
    for i in range(num_vehicles):
        prefix_num = i // (16 ** 4)
        hex_num = i % (16 ** 4)
        prefix = ''.join(prefix_chars[(prefix_num // (26 ** j)) % 26] for j in reversed(range(3)))
        vehicle_id = f"{prefix}{hex_num:04X}"
        vehicle_ids.append(vehicle_id)
    return vehicle_ids

# Function to generate sparse sensor data
def generate_sparse_sensor_data(vehicle_id, timestamp, num_sensors=110):
    data = {'vehicle_id': vehicle_id, 'timestamp': timestamp}

    # Determine how many sensors will send data this time
    num_active_sensors = max(1, random.randint(1, max(1, int(num_sensors * 0.1))))  # Ensure at least 1 sensor is active
    active_sensors = random.sample(range(1, num_sensors + 1), num_active_sensors)

    for sensor_id in active_sensors:
        data[f'sensor_{sensor_id}'] = random.uniform(0, 100)
    return data

def main(num_vehicles, num_sensors, delay_ms, total_rows, select_limit, csv_file, skip_ingestion, show_query, drop_table):
    # Database connection parameters for QuestDB
    db_params = {
        'dbname': 'qdb',
        'user': 'admin',
        'password': 'quest',
        'host': 'localhost',
        'port': '8812'
    }

    if not skip_ingestion:
        # Generate vehicle IDs
        vehicle_ids = generate_vehicle_ids(num_vehicles)

        # Connect to the PostgreSQL database (QuestDB in this case)
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        if drop_table:
            drop_table_query = 'DROP TABLE IF EXISTS vehicle_sensor_data'
            cursor.execute(drop_table_query)
            conn.commit()

        # Create the wide table for sensor data
        create_table_query = f'''
        CREATE TABLE IF NOT EXISTS vehicle_sensor_data (
            vehicle_id SYMBOL CAPACITY {num_vehicles} INDEX,
            timestamp TIMESTAMP,
            ''' + ', '.join([f'sensor_{i} FLOAT' for i in range(1, num_sensors + 1)]) + '''
        ) timestamp(timestamp) PARTITION BY HOUR WAL
        '''
        cursor.execute(create_table_query)
        conn.commit()

        # ILP protocol configuration
        ilp_conf = f'http::addr=localhost:9000;'

        with Sender.from_conf(ilp_conf) as sender:
            # Simulate data changes
            start_time = datetime.now()
            rows_generated = 0
            vehicle_timestamps = {vehicle_id: start_time for vehicle_id in vehicle_ids}

            while rows_generated < total_rows:
                for vehicle_id in vehicle_ids:
                    timestamp = vehicle_timestamps[vehicle_id]
                    sensor_data = generate_sparse_sensor_data(vehicle_id, timestamp, num_sensors=num_sensors)
                    symbols = {'vehicle_id': vehicle_id}
                    columns = {k: v for k, v in sensor_data.items() if k != 'timestamp' and k != 'vehicle_id'}
                    timestamp_nanos = int(timestamp.timestamp() * 1e9)
                    sender.row(
                        'vehicle_sensor_data',
                        symbols=symbols,
                        columns=columns,
                        at=TimestampNanos(timestamp_nanos)
                    )
                    vehicle_timestamps[vehicle_id] += timedelta(milliseconds=delay_ms)
                    rows_generated += 1
                    if rows_generated >= total_rows:
                        break

        # Close the connection
        cursor.close()
        conn.close()

    # Construct the query
    query = '''
    SELECT
        timestamp,
        vehicle_id,
        ''' + ', '.join([f'last_not_null(sensor_{i}) AS sensor_{i}' for i in range(1, num_sensors + 1)]) + '''
    FROM
        vehicle_sensor_data
    -- WHERE vehicle_id = 'AAA0000'
    SAMPLE BY 1m
    LIMIT %s;
    ''' % select_limit

    # Show the SQL query if show_query is true
    if show_query:
        print("SQL Query:")
        print(query)

    # If CSV file path is provided, execute the query and write results to CSV
    if csv_file:
        # Connect to the PostgreSQL database (QuestDB in this case)
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()

        cursor.execute(query)
        results = cursor.fetchall()

        # Output the results to the specified CSV file
        with open(csv_file, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile)
            # Write the header
            header = ['timestamp', 'vehicle_id'] + [f'avg_sensor_{i}' for i in range(1, num_sensors + 1)]
            csvwriter.writerow(header)
            # Write the data rows
            csvwriter.writerows(results)

        # Close the connection
        cursor.close()
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate sensor data for vehicles and insert into QuestDB")
    parser.add_argument('--num_vehicles', type=int, default=10, help='Number of vehicles to simulate')
    parser.add_argument('--num_sensors', type=int, default=110, help='Number of sensors per vehicle')
    parser.add_argument('--delay_ms', type=int, default=100, help='Delay between data insertions for the same device in milliseconds')
    parser.add_argument('--total_rows', type=int, default=1000000, help='Total number of rows to generate')
    parser.add_argument('--select_limit', type=int, default=100, help='Limit for the number of rows to select in the query')
    parser.add_argument('--csv', type=str, help='CSV file path to save the query results')
    parser.add_argument('--skip_ingestion', action='store_true', help='Skip data ingestion step')
    parser.add_argument('--show_query', type=bool, default=True, help='Show the SQL query')
    parser.add_argument('--drop_table', action='store_true', help='Drop the table before creating it')
    args = parser.parse_args()

    main(args.num_vehicles, args.num_sensors, args.delay_ms, args.total_rows, args.select_limit, args.csv, args.skip_ingestion, args.show_query, args.drop_table)
