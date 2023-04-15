# Weather-Data-Management

The weather data system supports storage of temperature and humidity values from various devices. It also has a device registry and user information.

The data is stored in the MySQL database. The focus of the project is to learn some data modeling in MySQL, and to implement various functionalities based on that.

Each device sends either temperature or humidity data based on its type.


The simple program is structured in various layers.

 

Data model:
devices table: This stores information about the devices. It has device_id (String), description (String), device_type (String - temperature/humidity) and manufacturer (String) fields. Each device generates only one type of data.
 
weather_data table: This stores the actual weather data. It contains device_id (String), data_value (Decimal), and data_timestamp (Date) fields in each document.
 
daily_report table: This table’s data is generated on the fly, when aggregation operation is performed on the weather_data table to create daily report summaries. It contains device_id (String), avg_value (Decimal), max_value (Decimal), min_value (Decimal), and report_date (Date).
