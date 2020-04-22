""" transfer

A module for insertion and extraction of data with MySQL databases.

Classes
-------
DBColumn
    A container for one column of data in a database table.
DBTable
    A container of data for a database table.
DBAssist
    A tool for exchanging data with a MySQL database using pymysql.

"""
import ConfigParser
import os
import json
import math
import pandas as pd
import numpy as np
import pymysql
from src.constants import SCHEMA_FILE
from src.constants import CONFIG_FILE


class DBColumn(object):
    """
    A container for a column of data in DBTable.

    Attributes
    ----------
    data : list
        Column data assigned as input to the class instance. First element is
        str column name.
    name: str
        Name of the column.
    values: list
        List of element values.
    type: str
        String defining the column type for MySQL table.
    query: str
        String containing the column name and type for CREATE/INSERT query.

    """
    def __init__(self, data):
        """Initialize DBColumn instance."""
        self.data = data
        self.name = self.data[0].replace('-', '_')
        self.values = self.data[1:]
        self.type = self.column_type()
        self.query = "".join([self.name, self.type])

    def column_type(self):
        """Return string indicating MYSQL type to use for column."""
        # collect all unique types as strings
        elem_types = list(set([type(x).__name__ for x in self.values]))

        # if any strings use VARCHAR, otherwise use FLOAT
        if 'str' in elem_types:
            # keep len at 64, sufficient size for expected values
            col_type = " VARCHAR (64)"
        else:
            col_type = " FLOAT"

        return col_type

    def convert_column(self):
        """
        Return list of column values converted for MySQL insertion.
    
        Returns
        -------
        new_values : list of str
            String values converted from numeric or string types. None, 
            empty strings, or NaN are converted to NULL.
        
        """
        new_values = [self.convert_element(x) for x in self.values]
        nulls = ['nan', '', 'None', "'nan'", "''", "'None'"]
        new_values = [x if x not in nulls else 'NULL' for x in new_values]
        return new_values

    def convert_element(self, raw_value):
        """
        Return element as a formatted string for insertion to MySQL table.

        Parameters
        ----------
        raw_value : str, float, int, NaN, or None
            The original cell value when DBTable is initialized.

        Returns
        -------
        new_value : str
            Value converted to string.

        """
        # code nulls separately to prevent inserting as strings
        if "VARCHAR" in self.type:
            # convert any numerics to string, keep any existing ' intact
            new_value = str(raw_value).replace("'", r"\'")
            new_value = r"""'{}'""".format(new_value)
        else:
            new_value = """{}""".format(raw_value)

        return new_value


class DBTable(object):
    """
    A container for 2-dimensional data for insertion to MySQL table.

    Attributes
    ----------
    name : str
        Name of the table in MySQL database.
    data : list of list
        First list must be column names, remaining lists contain rows of data.
    columns: list of DBColumn
        Contains DBColumn instance for each column in the table.

    """
    def __init__(self, name, data):
        """Initialize DBTable instance."""
        self.name = name
        self.data = self.setup(data)
        # initialize all columns to get names, types, and values
        self.columns = [DBColumn(x) for x in map(list, zip(*self.data))]

    def setup(self, data):
        """Return a DBTable instance created from the data in a nested list or 
        pandas DataFrame."""
        if type(data) is list:
            result = data
        elif type(data) is pd.core.frame.DataFrame:
            col_names = list(data.columns)
            result = data.values.tolist()
            result.insert(0, col_names)
        else:
            raise Exception('data must be list or DataFrame')

        return result

    def query_create(self):
        """Return query for creating table in MySQL database."""
        query_columns = ", \n".join([c.query for c in self.columns])
        pref = "CREATE TABLE "
        #create = """ """.join([pref, self.name, "(", query_columns, ");"])
        create = "{0} {1} ({2});".format(pref, self.name, query_columns)
        return create

    def query_insert(self, at_once=True):
        """
        Return query for inserting data to table in MySQL database.
        
        Parameters
        ----------
        at_once : bool, default True
            Return a single formatted string for inserting all rows at once.
            If False, return a list of separate strings for each row.

        Returns
        -------
        insert : str or list of str
            Str or list for inserting table values to MySQL database.

        """
        # create prefix string for insert query
        col_string = ", ".join(self.data[0])
        pref = "INSERT INTO {0} ({1}) VALUES".format(self.name, col_string)
        
        # list with formatted string for each row
        table_rows = self.query_rows()
        
        if at_once is True:
            rows_combined = ",\n".join(table_rows)
            insert = "{0} {1};".format(pref, rows_combined)
        else:
            insert = ["{0} {1};".format(pref, row) for row in table_rows]

        return insert

    def query_rows(self):
        """Return table rows as list of formatted strings."""
        column_values = [c.convert_column() for c in self.columns]
        rows = map(list, zip(*column_values))
        row_strings = [", ".join(row_list) for row_list in rows]
        rows_formatted = ["({0})".format(row) for row in row_strings]
        return rows_formatted


class DBAssist():
    """
    A tool for efficient exchanges with MySQL using the pymysql package.

    Attributes
    ----------
    conn : pymysql.connection
        A pymysql connection with MySQL database.
    cursor : pymysql.cursor
        A pymysql cursor called from the conn attribute.

    """
    def __init__(self, conn=None, cursor=None):
        self.conn = conn
        self.cursor = cursor

    def connect(self, config_file=CONFIG_FILE):
        """
        Establish connection with database.
        
        Parameters
        ----------

        """
        parser = ConfigParser.ConfigParser()
        parser.readfp(open(config_file))
        
        self.conn = pymysql.connect(host='127.0.0.1',
                                    port=3306,
                                    user='root',
                                    passwd=parser.get('Local', 'pwd'),
                                    db=parser.get('Local', 'db'))
        self.cursor = self.conn.cursor()

    def close(self):
        """Close cursor and connection with database."""
        self.cursor.close()
        self.conn.close()

    def schema_query(self, table_name, schema_file=SCHEMA_FILE):
        """
        Return query used to create MySQL table using the specified schema.

        Parameters
        ----------
        table_name : str
            Name of the table as specified in the json file.
        schema_file : str
            Location of json file with table specs. Default is imported from
            src/constants.

        Returns
        -------
        query_create : str
            Text of valid MySQL query to create table.

        """
        with open(schema_file, 'r') as f:
            schema = json.load(f)[table_name]
        
        # join the column portion of the create query
        cols = ["{0} {1}".format(c['name'], c['type']) for c in schema]
        col_part = ", \n".join(cols)

        # single str object for the full table
        query_create = "CREATE TABLE {0} ({1});".format(table_name, col_part)
        return query_create

    def create_table(self, query_create):
        """
        Execute query used to create a table.

        Parameters
        ----------
        query_create : str
            
        """
        self.connect()
        try:
            self.cursor.execute(query_create)
        except pymysql.err.InternalError, e:
            print e
        self.close()

    def create_from_schema(self, table_name):
        """
        Create MySQL table from parameters specified in json file.

        Parameters
        ----------
        table_name: str
            Name of table. Must be included as key in schema_file.

        """
        query_create = self.schema_query(table_name)
        self.create_table(query_create)

    def create_from_data(self, name, data):
        """
        Create MySQL table by extracting names and types from sample of data.

        Parameters
        ----------
        name : str
            Name to assign the table in MySQL database.
        data : list of list or pandas DataFrame

        """
        query_create = DBTable(name, data).query_create()
        self.create_table(query_create)

    def execute_commit(self, query):
        """
        Execute a query and commit changes to database.

        Helpful to iterate over lists of insert queries so that unforeseen 
        errors don't kill the script.

        Parameters
        ----------
        query : str
            Text of a MySQL query to execute.
        """
        try:
            self.cursor.execute(query)
            self.conn.commit()
        except pymysql.err.DataError, e:
            print e

    def insert_rows(self, table_name, data, at_once=True):
        """
        Insert new rows to database.

        Parameters
        ----------
        name : str
            Name of table in database where rows are inserted.
        data : list of list or pandas DataFrame
            Data to insert as new rows. 
        at_once : bool
            Insert rows and commit changes as a single query. If False, insert
            each row and commit changes separately.

        """
        table = DBTable(table_name, data)

        self.connect()

        # insert full table at once
        if at_once == True:
            query_one = table.query_insert(at_once=at_once)
            self.execute_commit(query_one)
        # insert one row at a time
        else:
            query_list = table.query_insert(at_once=at_once)
            map(lambda x: self.execute_commit(x), query_list)

        self.close()

    def delete_rows(self, table_name):
        """
        Delete all rows in a table in database.

        Parameters
        ----------
        table_name : str
            Name of the table in database.

        """
        query_delete = "DELETE from {}".format(table_name)
        self.connect()
        self.execute_commit(query_delete)
        self.close()

    def replace_rows(self, table_name, data, at_once=True):
        """
        Replace all rows in a table in database.

        Parameters
        ----------
        table_name : str
            Name of table in database where rows are inserted.
        data : list of list or pandas DataFrame
            Data to insert as new rows. 
        at_once : bool
            Insert rows and commit changes as a single query. If False, insert
            each row and commit changes separately.

        """
        self.delete_rows(table_name)
        self.insert_rows(table_name, data, at_once=at_once)

    def query_result(self, query, as_dict=True):
        """
        Return the results of a query on database.

        Parameters
        ----------
        query : str
            Text of a valid MySQL query.
        as_dict : bool, default True
            Return list of dict with value labels as keys for each dict.
            If False, return tuple of tuples with tuple elements in order.

        Returns
        -------
        result : list of dict, tuple of tuple
            Result of the query.

        """
        self.connect()
        
        if as_dict == True:
            self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)

        self.cursor.execute(query)
        result = self.cursor.fetchall()
        self.close()
        return result

    def table_columns(self, table_name):
        """
        Return the column names from table in database.

        Parameters
        ----------
        table_name : str
            Name of table in database for retrieving column names.

        Returns
        -------
        column_names : list of str
            Names of columns in database table.

        """
        query = """SHOW COLUMNS FROM %s;""" % (table_name)
        result = self.query_result(query, as_dict=False)
        column_names = [col[0] for col in result]
        return column_names

    def return_data(self, table_name, subset=None, modifier=""):
        """
        Return the data from table in database.

        Parameters
        ----------
        table_name : str
            Name of table in database for retreiving data.
        subset : str or list of str, default None, optional
            Name of table column or list of columns to return.
        modifier : str, default "", optional
            Text of MySQL WHERE statement used to select rows to return.

        Returns
        -------
        df : pandas DataFrame
            Contents of table returned as dataframe.

        """
        if subset is None:
            columns_to_get = '*'
        elif type(subset) is str:
            columns_to_get = subset
        elif type(subset) is list:
            columns_to_get = ", ".join(subset)
        else:
            sub_type = type(subset)
            msg = 'Subset must be None, str, or list, not {}'.format(sub_type)
            raise Exception(msg)

        # list of column indices where type is decimal
        query = "SELECT {0} FROM {1} {2};".format(columns_to_get, table_name,
                                                  modifier)
        result = self.query_result(query)
        df = pd.DataFrame(result)
        
        return df
