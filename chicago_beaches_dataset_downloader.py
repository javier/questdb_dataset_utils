import requests
import json
import os

def download_from_url(url, filename):
    print(f'Downloading {url} into {filename}')
    download = requests.get(url)
    with open(filename, 'wb') as csv_file:
        csv_file.write(download.content)

def upload_table(host, from_file, table_name):
    print(f'Uploading {from_file} into {table_name}')
    params = {'name': table_name, 'overwrite': 'true', }
    files = {'data': open(from_file, 'rb'),}
    try:
        r = requests.post(host + '/imp', params=params, files=files)
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}')

def run_query(host, sql_query):
    query_params = {'query': sql_query, 'fmt' : 'json'}
    try:
        response = requests.get(host + '/exec', params=query_params)
        json_response = json.loads(response.text)
        #print(json_response)
    except requests.exceptions.RequestException as e:
        print(f'Error: {e}')

def transform_locations_table(host, file_name, table_name):
    create_query = f"""
        CREATE TABLE IF NOT EXISTS '{table_name}' (
            UpdatedAt TIMESTAMP,
            SensorName SYMBOL,
            SensorType SYMBOL,
            Latitude DOUBLE,
            Longitude DOUBLE,
            G7c GEOHASH(7c)
            ) timestamp(UpdatedAt) PARTITION BY YEAR WAL;
            """
    truncate_query = f"""
        TRUNCATE TABLE '{table_name}'
    """

    transform_query = f"""
        INSERT INTO '{table_name}'
        SELECT
                '2023-09-04T00:00:00.000000Z' AS UpdatedAt,
                SensorName,
                SensorType,
                Latitude,
                Longitude,
                make_geohash(Longitude, Latitude, 35) as G7c
        FROM "{file_name}";
    """

    drop_query = f"""
        DROP TABLE  "{file_name}";
    """

    run_query(host, create_query)
    run_query(host, truncate_query)
    run_query(host, transform_query)
    run_query(host, drop_query)


def transform_water_table(host, file_name, table_name):
    create_query = f"""
        CREATE TABLE IF NOT EXISTS '{table_name}' (
            MeasurementTimestamp TIMESTAMP,
            BeachName SYMBOL,
            WaterTemperature DOUBLE,
            Turbidity DOUBLE,
            TransducerDepth DOUBLE,
            WaveHeight DOUBLE,
            WavePeriod INT,
            BatteryLife DOUBLE,
            MeasurementTimestampLabel STRING,
            MeasurementID STRING
            ) timestamp(MeasurementTimestamp) PARTITION BY MONTH WAL;
            """
    truncate_query = f"""
        TRUNCATE TABLE '{table_name}'
    """

    transform_query = f"""
        INSERT INTO '{table_name}'
        SELECT  to_timestamp(CONCAT(MeasurementTimestamp,' CST'), 'MM/dd/yyyy kk:mm:ss a z') as MeasurementTimestamp,
                BeachName,
                WaterTemperature,
                Turbidity,
                CAST(TransducerDepth AS DOUBLE) AS TransducerDepth,
                WaveHeight,
                WavePeriod,
                BatteryLife,
                MeasurementTimestampLabel,
                MeasurementID
        FROM "{file_name}";
    """

    drop_query = f"""
        DROP TABLE  "{file_name}";
    """

    run_query(host, create_query)
    run_query(host, truncate_query)
    run_query(host, transform_query)
    run_query(host, drop_query)


def transform_weather_table(host, file_name, table_name):
    create_query = f"""
        CREATE TABLE IF NOT EXISTS '{table_name}' (
            MeasurementTimestamp TIMESTAMP,
            StationName SYMBOL,
            AirTemperature DOUBLE,
            WetBulbTemperature DOUBLE,
            Humidity INT,
            RainIntensity DOUBLE,
            IntervalRain DOUBLE,
            TotalRain DOUBLE,
            PrecipitationType INT,
            WindDirection INT,
            WindSpeed DOUBLE,
            MaximumWindSpeed DOUBLE,
            BarometricPressure DOUBLE,
            SolarRadiation INT,
            Heading INT,
            BatteryLife DOUBLE,
            MeasurementTimestampLabel STRING,
            MeasurementID STRING
            ) timestamp(MeasurementTimestamp) PARTITION BY MONTH WAL;
            """
    truncate_query = f"""
        TRUNCATE TABLE '{table_name}'
    """

    transform_query = f"""
        INSERT INTO '{table_name}'
        SELECT  to_timestamp(CONCAT(MeasurementTimestamp,' CST'), 'MM/dd/yyyy kk:mm:ss a z') as MeasurementTimestamp,
                StationName,
                AirTemperature, WetBulbTemperature, Humidity, RainIntensity , IntervalRain,
                TotalRain,
                PrecipitationType,
                WindDirection,
                WindSpeed,
                MaximumWindSpeed,
                BarometricPressure,
                SolarRadiation,
                Heading,
                BatteryLife,
                MeasurementTimestampLabel,
                MeasurementID
        FROM "{file_name}";
    """

    drop_query = f"""
        DROP TABLE  "{file_name}";
    """

    run_query(host, create_query)
    run_query(host, truncate_query)
    run_query(host, transform_query)
    run_query(host, drop_query)

if __name__ == '__main__':
    host = 'http://localhost:9000'

    sensor_location_url = 'https://data.cityofchicago.org/api/views/g3ip-u8rb/rows.csv'
    sensor_location_file_name = 'Beach_Water_and_Weather_Sensor_Locations.csv'
    sensor_location_table_name = 'chicago_sensor_locations'
    download_from_url(sensor_location_url, sensor_location_file_name)
    upload_table(host, sensor_location_file_name, sensor_location_file_name)
    transform_locations_table(host, sensor_location_file_name, sensor_location_table_name)
    os.remove(sensor_location_file_name)

    water_quality_url = 'https://data.cityofchicago.org/api/views/qmqz-2xku/rows.csv'
    water_quality_file_name = 'Beach_Water_Quality_-_Automated_Sensors.csv'
    water_table_name = 'chicago_water_sensors'
    download_from_url(water_quality_url, water_quality_file_name)
    upload_table(host, water_quality_file_name, water_quality_file_name)
    transform_water_table(host, water_quality_file_name, water_table_name)
    os.remove(water_quality_file_name)

    weather_quality_url = 'https://data.cityofchicago.org/api/views/k7hf-8y75/rows.csv'
    weather_quality_file_name = 'Beach_Weather_Stations_-_Automated_Sensors.csv'
    weather_table_name = 'chicago_weather_stations'
    download_from_url(weather_quality_url, weather_quality_file_name)
    upload_table(host, weather_quality_file_name, weather_quality_file_name)
    transform_weather_table(host, weather_quality_file_name, weather_table_name)
    os.remove(weather_quality_file_name)





