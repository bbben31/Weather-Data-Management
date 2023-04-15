from sys import settrace
from database import Database
import datetime
import math

column_compare = {
    'EQUAL_TO': '=',
    'GREATER_THAN': '>',
    'GREATER_THAN_OR_EQUAL_TO': '>=',
    'LESSER_THAN': '<',
    'LESSER_THAN_OR_EQUAL_TO': '<='
}

#############################################################################################################
# The model layer class that interfaces with the devices table in the MySQl database.
# It provides functions that takes in data values used for CRUD operations on the table.
# The data values are then passed on to the database layer, with additional table-specific and query-
# specific information - to dynamically construct queries, and execute them.
#############################################################################################################

class DeviceModel:
    DEVICE_TABLE = 'devices'

    def __init__(self, db_config):
        self._db_config = db_config
        self._db = Database(db_config)
        self._latest_error = ''

    @property
    def latest_error(self):
        return self._latest_error

    @latest_error.setter
    def latest_error(self, latest_error):
        self._latest_error = latest_error

#############################################################################################################
# A function to retrieve a single (unique) devices table entry for a particular device_id.
#
# It populates a query_columns_dict dictionary with key and value as follows:
#   key: The column name relevant to the query
#   value: a tuple consisting of - (Comparison operator, value to match)
#
# It invokes the appropriate function exposed by the database layer
#############################################################################################################

    def find_by_device_id(self, device_id):
        query_columns_dict = {
            'device_id': (column_compare['EQUAL_TO'], device_id)
        }

        result = self._db.get_single_data(DeviceModel.DEVICE_TABLE, query_columns_dict)
        return result

#############################################################################################################
# A function to insert a single row into the devices table.
#
# It takes the values corresponding to a single row in the table, and invokes the appropriate function 
# exposed by the database layer - only if the entry does not exist already!
# 
# It populates a query_columns_dict dictionary with key and value as follows:
#   key: The column name relevant to the query
#   value: a tuple consisting of - (Comparison operator, value to match)
#############################################################################################################

    def insert(self, device_id, desc, type, manufacturer):
        self.latest_error = ''
        result = self.find_by_device_id(device_id)

        if (result):
            self.latest_error = f'Device id {device_id} already exists!'
            return -1

        query_columns_dict = {
            'device_id': device_id, 
            'description': desc, 
            'device_type': type, 
            'manufacturer': manufacturer
        }

        row_count = self._db.insert_single_data(DeviceModel.DEVICE_TABLE, query_columns_dict)
        return row_count

#############################################################################################################
# The model layer class that interfaces with the weather_data table in the MySQl database.
# It provides functions that takes in data values used for CRUD operations on the table.
# The data values are then passed on to the database layer, with additional table-specific and query-
# specific information - to dynmaically construct queries, and execute them.
#############################################################################################################

class WeatherDataModel:
    WEATHER_DATA_TABLE = 'weather_data'

    def __init__(self, db_config):
        self._db_config = db_config
        self._db = Database(db_config)
        self._latest_error = ''
        
    @property
    def latest_error(self):
        return self._latest_error

    @latest_error.setter
    def latest_error(self, latest_error):
        self._latest_error = latest_error
    
#############################################################################################################
# A function to retrieve multiple weather_data table entries that match a particular device_id.
#
# It populates a query_columns_dict dictionary with key and value as follows:
#   key: The column name relevant to the query
#   value: a tuple consisting of - (Comparison operator, value to match)
#
# It invokes the appropriate function exposed by the database layer
#############################################################################################################

    def find_multiple_by_device_id(self, device_id):
        query_columns_dict = {
            'device_id': (column_compare['EQUAL_TO'], device_id)
        }

        result = self._db.get_multiple_data(WeatherDataModel.WEATHER_DATA_TABLE, query_columns_dict)
        return result

#############################################################################################################
# A function to retrieve a single weather_data table entry that matches both: 
#    1. A particular device_id.
#    2. A specific timestamp value.
#
# It populates a query_columns_dict dictionary with key and value as follows:
#   key: The column name relevant to the query
#   value: a tuple consisting of - (Comparison operator, value to match)
#
# It invokes the appropriate function exposed by the database layer
#############################################################################################################

    def find_by_device_id_and_timestamp(self, device_id, timestamp):
        val_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')

        query_columns_dict = {
            'device_id': (column_compare['EQUAL_TO'], device_id),
            'data_timestamp': (column_compare['EQUAL_TO'], val_timestamp)
        }

        result = self._db.get_single_data(WeatherDataModel.WEATHER_DATA_TABLE, query_columns_dict)
        return result
        
#############################################################################################################
# A function to retrieve a single weather_data table entry that matches both: 
#    1. A particular device_id.
#    2. All values that lie in a specific range - between low_value and high_value - both non-inclusive.
#
# It populates a query_columns_dict dictionary with key and value as follows:
#   key: The column name relevant to the query
#   value: a tuple consisting of - (Comparison operator, value to match)
#
# It invokes the appropriate function exposed by the database layer
#############################################################################################################

    def find_by_device_id_and_value(self, device_id, low_value, high_value):
        query_columns_dict = {
            'device_id': (column_compare['EQUAL_TO'], device_id),
            'data_value': (column_compare['GREATER_THAN'], low_value),
            'data_value': (column_compare['LESSER_THAN'], high_value)
        }

        result = self._db.get_single_data(WeatherDataModel.WEATHER_DATA_TABLE, query_columns_dict)
        return result

#############################################################################################################
# A function to retrieve all the rows of the weather_data table.
#
# It achieves this by passing a value of None for the expected query_columns_dict parameter, when it
#   invokes the appropriate function exposed by the database layer
#############################################################################################################

    def find_all(self):
        results = self._db.get_multiple_data(WeatherDataModel.WEATHER_DATA_TABLE, None)
        return results
    
#############################################################################################################
# A function to insert a single row into the weather_data table.
#
# It takes the values corresponding to a single row in the table, and invokes the appropriate function 
# exposed by the database layer - only if the entry does not exist already (by matching device_id and timestamp)
# 
# It populates a query_columns_dict dictionary with key and value as follows:
#   key: The column name relevant to the query
#   value: a tuple consisting of - (Comparison operator, value to match)
#############################################################################################################

    def insert(self, device_id, value, timestamp):
        self._latest_error = ''
        
        result = self.find_by_device_id_and_timestamp(device_id, timestamp)
        
        if (result):
            self.latest_error = f'Data for timestamp {timestamp} for device id {device_id} already exists'
            return -1

        val_timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        
        query_columns_dict = {
            'device_id': device_id, 
            'data_value': value, 
            'data_timestamp': val_timestamp
        }

        row_count = self._db.insert_single_data(WeatherDataModel.WEATHER_DATA_TABLE, query_columns_dict)
        return row_count


class DailyReportModel:
    DAILY_REPORT_TABLE = 'daily_report'
    
    WD_DEVICE_ID_COL = 1
    WD_VALUE_COL = 2
    WD_TIMESTAMP_COL = 3

    def __init__(self, db_config):
        self._db_config = db_config
        self._db = Database(db_config)
        self._latest_error = ''
    
    @property
    def latest_error(self):
        return self._latest_error

    @latest_error.setter
    def latest_error(self, latest_error):
        self._latest_error = latest_error
    
#############################################################################################################
# A function to retrieve a single daily_report table entry that matches both: 
#    1. A particular device_id.
#    2. A specific date value.
#
# It populates a query_columns_dict dictionary with key and value as follows:
#   key: The column name relevant to the query
#   value: a tuple consisting of - (Comparison operator, value to match)
#
# It invokes the appropriate function exposed by the database layer
#############################################################################################################

    def find_by_device_id_and_date(self, device_id, date):
        val_date = date.strftime('%Y-%m-%d %H:%M:%S')
        
        query_columns_dict = {
            'device_id': (column_compare['EQUAL_TO'], device_id),
            'report_date': (column_compare['EQUAL_TO'], val_date)
        }

        result = self._db.get_single_data(DailyReportModel.DAILY_REPORT_TABLE, query_columns_dict)
        return result
    
#############################################################################################################
# A function to retrieve a single daily_report table entry that matches both: 
#    1. A particular device_id.
#    2. All dates that lie in a specific range - between from_date and to_date - both inclusive.
#
# It populates a query_columns_dict dictionary with key and value as follows:
#   key: The column name relevant to the query
#   value: a tuple consisting of - (Comparison operator, value to match)
#
# It invokes the appropriate function exposed by the database layer
#############################################################################################################

    def find_by_device_id_and_date_range(self, device_id, from_date, to_date):

        val_from_date = from_date.strftime('%Y-%m-%d %H:%M:%S')
        val_to_date = to_date.strftime('%Y-%m-%d %H:%M:%S')

        query_columns_dict = {
            'device_id': (column_compare['EQUAL_TO'], device_id),
            'report_date': (column_compare['GREATER_THAN_OR_EQUAL_TO'], val_from_date),
            'report_date': (column_compare['LESSER_THAN_OR_EQUAL_TO'], val_to_date)
        }

        results = self._db.get_multiple_data(DailyReportModel.DAILY_REPORT_TABLE, query_columns_dict)
        return results

#############################################################################################################
# A function to retrieve all the rows of the daily_report table.
#
# It achieves this by passing a value of None for the expected query_columns_dict parameter, when it
#   invokes the appropriate function exposed by the database layer
#############################################################################################################

    def find_all(self):
        results = self._db.get_multiple_data(DailyReportModel.DAILY_REPORT_TABLE, None)
        return results

#############################################################################################################
# A function to insert multiple rows into the daily_report table.
#
# It takes the values corresponding to the multiple rows as an array of tuples - and invokes the appropriate function 
# exposed by the database layer.
# 
# It populates a query_columns array with the table column names.
#############################################################################################################

    def insert_multiple(self, daily_report_docs):
        query_columns = [
            'device_id',
            'avg_value',
            'min_value',
            'max_value',
            'report_date'
        ]

        row_count = self._db.insert_multiple_data(DailyReportModel.DAILY_REPORT_TABLE, query_columns, daily_report_docs)
        return row_count

#################################################################################################################
# A function to aggregate all the multiple rows present in the weather_data table - into the daily_report table.
# As part of the aggrgation, the following value are computed:
#  1. avg_value
#  2. min_value
#  3. max_value
#
# This is done so that there is one entry corresponding per device, for each day - its daily report!
#################################################################################################################

    def aggregate_data(self):
        agg_data = {}

        weather_data_model = WeatherDataModel(self._db_config)

        for data in weather_data_model.find_all():
            device_id = data[DailyReportModel.WD_DEVICE_ID_COL]
            date = data[DailyReportModel.WD_TIMESTAMP_COL].date()
            value = data[DailyReportModel.WD_VALUE_COL]
            
            if (device_id not in agg_data):
                agg_data[device_id] = {}
            if (date not in agg_data[device_id]):
                agg_data[device_id][date] = {'sum': 0, 'count': 0, 'min': math.inf, 'max': -math.inf}
            
            agg_data[device_id][date]['sum'] += value
            agg_data[device_id][date]['count'] += 1
                    
            if (value < agg_data[device_id][date]['min']):
                agg_data[device_id][date]['min'] = value
            
            if (value > agg_data[device_id][date]['max']):
                agg_data[device_id][date]['max'] = value
        
        report_data = []
        for device_id in agg_data:
            for date in agg_data[device_id]:
                report_doc = (
                    device_id, 
                    round(agg_data[device_id][date]['sum'] / agg_data[device_id][date]['count'], 2), 
                    agg_data[device_id][date]['min'], 
                    agg_data[device_id][date]['max'], 
                    datetime.datetime(date.year, date.month, date.day)
                )

                report_data.append(report_doc)
        
        return report_data

#################################################################################################################
# A function to trigger the aggregation procedure, and populate the daily_report table.
#################################################################################################################

    def create_reports(self):
        daily_reports = self.find_all()
        if (daily_reports):
            print("Reports already created, skipping this step")
            return True

        self.latest_error = ''

        report_data = self.aggregate_data()

        self.insert_multiple(report_data)        
        return True
