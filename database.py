import mysql.connector

#############################################################################################################
# The database later class, that exposes utility functions that are generic 
# These functions are called by the different Models in the model layer
#############################################################################################################

class Database:

    def __init__(self, db_config):
        self.db_handle = mysql.connector.connect(
                user=db_config['username'],
                password=db_config['password'],
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['db_name']
            )
        
        self.mycursor = self.db_handle.cursor(buffered=True)

#############################################################################################################
# A function to retrieve a single row from the specified table (the first such row encountered, in case 
# of multiple row matches).
# 
# Function parameters:
# 1. table: The table to be queried
# 2. query_columns_dict: A dictionary that specifies the SELECT query matching clauses
#                        One of several comparison types can be specified (=, <, >, <=, or >=)
#                        for each column, along with the match value for that column.
# 
# Logic:
#  The function dynamically constructs an SQL SELECT query with a WHERE clause, using the columns, comparison
#  type, and the value to compare for each column - and queries the MySQL database.
#  The first such matching row is returned.
#############################################################################################################

    def get_single_data(self, table, query_columns_dict):
        selection_list = " AND ".join([
                                        f"{column_name} {query_columns_dict[column_name][0]} %s" 
                                        for column_name in sorted(query_columns_dict.keys())
                                        ])
        sql = f"SELECT * FROM {table} WHERE {selection_list}"
        
        val = tuple(query_columns_dict[column_name][1] for column_name in sorted(query_columns_dict.keys()))

        self.mycursor.execute(sql, val)
        result = self.mycursor.fetchone()

        return result

#############################################################################################################
# A function to retrieve a all matching rows from the specified table
# 
# Function parameters:
# 1. table: The table to be queried
# 2. query_columns_dict: A dictionary that specifies the SELECT query matching clauses
#                        One of several comparison types can be specified (=, <, >, <=, or >=)
#                        for each column, along with the match value for that column.
# 
# Logic:
#  The function dynamically constructs an SQL SELECT query with a WHERE clause, using the columns, comparison
#  type, and the value to compare for each column - and queries the MySQL database.
#  All matching rows are returned.
#
# If an empty parameter for query_columns_dict is passed in, then all the rows in the table are returned.
#############################################################################################################

    def get_multiple_data(self, table, query_columns_dict):
        if query_columns_dict == None:
            sql = f"SELECT * FROM {table}"
            self.mycursor.execute(sql)
        else:
            selection_list = " AND ".join([
                                            f"{column_name} {query_columns_dict[column_name][0]} %s" 
                                            for column_name in sorted(query_columns_dict.keys())
                                            ])
            sql = f"SELECT * FROM {table} WHERE {selection_list}"
            
            val = tuple(query_columns_dict[column_name][1] for column_name in sorted(query_columns_dict.keys()))
            self.mycursor.execute(sql, val)

        result = self.mycursor.fetchall()

        return result

#############################################################################################################
# A function to insert a single row into the specified table
# 
# Function parameters:
# 1. table: The table to be inserted into
# 2. query_columns_dict: A dictionary that specifies the INSERT query matching column names, along with the
#                        values to be inserted as a row - one value for each matching column name.
# 
# Logic:
#  The function dynamically constructs an SQL INSERT query using the column names, and the value given to 
#  each column - and runs it with the MySQL database.
#############################################################################################################

    def insert_single_data(self, table, query_columns_dict):
        column_names = ",".join([f"{column_name}" for column_name in sorted(query_columns_dict.keys())])
        column_holders = ",".join([f"%s" for column_name in sorted(query_columns_dict.keys())])
        sql = f"INSERT INTO {table} ({column_names}) VALUES ({column_holders})"

        val = tuple(query_columns_dict[column_name] for column_name in sorted(query_columns_dict.keys()))

        self.mycursor.execute(sql, val)
        self.db_handle.commit()

        return self.mycursor.rowcount

#############################################################################################################
# A function to insert multiple rows into the specified table
# 
# Function parameters:
# 1. table: The table to be inserted into
# 2. columns: An array that specifies the INSERT query matching column names.
# 3. multiple_data: An array of tuples, each tuple representing a singlw row that needs to be inserted
#                   into the table.
# 
# Logic:
#  The function dynamically constructs an SQL INSERT query using the column names, and passes in the array 
#  of tuples to be inserted as rows - and runs it with the MySQL database.
#############################################################################################################

    def insert_multiple_data(self, table, columns, multiple_data):
        column_names = ",".join(columns)
        column_holders = ",".join([f"%s" for column_name in columns])
        sql = f"INSERT INTO {table} ({column_names}) VALUES ({column_holders})"

        self.mycursor.executemany(sql, multiple_data)
        self.db_handle.commit()

        return self.mycursor.rowcount
