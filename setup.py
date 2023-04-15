import random
from datetime import datetime
import mysql.connector
import os
import json


CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__),'..', 'config'))
DB_CONFIG_FILE_PATH = os.path.join(CONFIG_PATH, 'db.json')

DEVICE_TABLE = 'devices'
WEATHER_DATA_TABLE = 'weather_data'
DAILY_REPORT_TABLE = 'daily_report'

#######################################################################################
# The initial database setup logic - to pre-load the database tables
#######################################################################################

db_config = {}
with open(DB_CONFIG_FILE_PATH) as db_fh:
    db_config = json.load(db_fh)

# Connect to the database by read in the configuration parameters from the JSON config file

db_handle = mysql.connector.connect(
        user=db_config['username'],
        password=db_config['password'],
        host=db_config['host'],
        port=db_config['port'],
    )

# Obtain a cursor to execute database queries

mycursor = db_handle.cursor()
mycursor.execute(f'SHOW DATABASES')
db_collection = mycursor.fetchall()

# Drop the database if it exists, start from scratch

for db in db_collection:
    if db[0] == db_config['db_name']:
        mycursor.execute(f'DROP DATABASE {db[0]}')
        break

# Create the database afresh

mycursor.execute(f'CREATE DATABASE {db_config["db_name"]}')
mycursor.execute(f'USE {db_config["db_name"]}')


# Create and populate the devices table by reading the devices.csv configuration file

mycursor.execute(f'CREATE TABLE devices (id INT NOT NULL AUTO_INCREMENT, device_id VARCHAR(15) NOT NULL UNIQUE, description VARCHAR(127), device_type VARCHAR(31) NOT NULL, manufacturer VARCHAR(63), PRIMARY KEY (id));')

with open(os.path.join(CONFIG_PATH, f'{DEVICE_TABLE}.csv'), 'r') as device_fh:
    for row in device_fh:
            row = row.rstrip()
            
            if row:
                    device_id, desc, type, manufacturer = row.split(',')
                    
                    sql = 'INSERT INTO devices (device_id, description, device_type, manufacturer) VALUES (%s, %s, %s, %s)'
                    val = (device_id, desc, type, manufacturer)
                    mycursor.execute(sql, val)

                    db_handle.commit()

# Create and populate the weather_data table by generating randomized data values corresponding to different 
# configured devices

mycursor.execute(f'CREATE TABLE weather_data (id INT NOT NULL AUTO_INCREMENT, device_id VARCHAR(31) NOT NULL, data_value DECIMAL(6,2), data_timestamp DATETIME, PRIMARY KEY (id), FOREIGN KEY (device_id) REFERENCES devices(device_id))')

with open(os.path.join(CONFIG_PATH, f'{DEVICE_TABLE}.csv'), 'r') as device_fh:
    for row in device_fh:
        row = row.rstrip()
        if row:
            (device_id, _, type, _) = row.split(',')

        for day in range(1,6):
            for hour in range(0,24):
                timestamp = datetime(2021, 12, day, hour, 30, 0)
                
                value = None

                if (type.lower() == 'temperature'):
                    value = round(random.normalvariate(24, 2.2), 1)
                elif (type.lower() == 'humidity'):
                    value = round(random.normalvariate(45, 3), 1)

                val_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                
                sql = 'INSERT INTO weather_data (device_id, data_value, data_timestamp) VALUES (%s, %s, %s)'
                val = (device_id, value, val_timestamp)

                mycursor.execute(sql, val)
                db_handle.commit()

# Create the daily_report table, but leave it empty for now
# A trigger to create daily reports will cause aggregation on the weather_data table, populating this table

mycursor.execute(f'CREATE TABLE daily_report (id INT NOT NULL AUTO_INCREMENT, device_id VARCHAR(31) NOT NULL, avg_value DECIMAL(6,2), min_value DECIMAL(6,2), max_value DECIMAL(6,2), report_date DATETIME, PRIMARY KEY (id), FOREIGN KEY (device_id) REFERENCES devices(device_id))')
