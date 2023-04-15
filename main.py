from datetime import datetime
import os
import json

from model import DeviceModel, WeatherDataModel, DailyReportModel

CONFIG_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__),'..', 'config'))
DB_CONFIG_FILE_PATH = os.path.join(CONFIG_PATH, 'db.json')
db_config = {}
with open(DB_CONFIG_FILE_PATH) as db_fh:
    db_config = json.load(db_fh)


#######################################################################################
# The client code logic - invokes the model layer functionality
#######################################################################################

device_model = DeviceModel(db_config)
weather_data_model = WeatherDataModel(db_config)
daily_report_model = DailyReportModel(db_config)

#######################################################################################
# CRUD Operations
#######################################################################################

print('Accessing device DT004')
device_data = device_model.find_by_device_id('DT004')
print(device_data, end='\n\n')


print('Creating device DH201')
device_res = device_model.insert('DH201', 'Humidity Sensor', 'Humidity', 'Acme')
if (device_res == -1):
    print(device_model.latest_error, end='\n\n')
else:
    print(f'Rows inserted: {device_res}', end='\n\n')
    device_data = device_model.find_by_device_id('DH201')
    print(device_data, end='\n\n')

print('Read all DH002 device weather data')
multi_weather_data = weather_data_model.find_multiple_by_device_id('DH002')
print(multi_weather_data, end='\n\n')

print('Read DT001 device weather data at a particular timestamp')
weather_data = weather_data_model.find_by_device_id_and_timestamp('DT001', 
                                                                            datetime(2021, 12, 2, 13, 30, 0))
print(weather_data, end='\n\n')

print('Read the first DT002 device weather data entry, in a temperature range')
one_weather_data = weather_data_model.find_by_device_id_and_value('DT002', 22, 26)
print(one_weather_data, end='\n\n')

print('Insert an entry into the weather_data table')
row_count = weather_data_model.insert('DH201', 24.2, datetime(2021, 12, 3, 15, 30, 0))
print(row_count, end='\n\n')

###############################################################################################
# Daily Report Aggregation
###############################################################################################

print('Generate daily reports', end='\n\n')
daily_report_model.create_reports()

print('Get daily report for one day')
daily_report = daily_report_model.find_by_device_id_and_date('DT004', 
                                                                datetime(2021, 12, 2))
print(daily_report, end='\n\n')

print('Get daily report for multiple days')
daily_reports = daily_report_model.find_by_device_id_and_date_range('DH004', 
                                                                        datetime(2021, 12, 2), 
                                                                        datetime(2021, 12, 4))
print(daily_reports, end='\n\n')
