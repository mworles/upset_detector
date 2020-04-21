""" transfer

A module for efficient interactions with MySQL database using python.

Classes
-------
DBColumn
    Define.
DBTable
    Define.
DBAssist
    Define.

Functions
---------
schema_create
    Define.
make_table
    Define.
df_list
    Define.

"""
import ConfigParser
import os
import json
import math
import pandas as pd
import numpy as np
import pymysql
import src.constants
from typing import TypeVar, Generic


class DBColumn():
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


class DBTable(Generic[TypeVar('T')]):
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
        self.data = data
        # initialize all columns to get names, types, and values
        self.columns = [DBColumn(x) for x in map(list, zip(*self.data))]

    def query_create(self):
        """Return query for creating table in MySQL database."""
        query_columns = ", \n".join([c.query for c in self.columns])
        pref = "CREATE TABLE IF NOT EXISTS"
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
    def __init__(self):
        self.conn = None
        self.cursor = None

    def connect(self, config_file=src.constants.CONFIG_FILE):
        """Establish connection with database."""
        parser = ConfigParser.ConfigParser()
        parser.readfp(open(config_file))
        pwd = parser.get('Local', 'pwd')
        db = parser.get('Local', 'db')
        self.conn = pymysql.connect(host='127.0.0.1',
                                    port=3306,
                                    user='root',
                                    passwd=pwd,
                                    db=db)
        self.cursor = self.conn.cursor()

    def close(self):
        """Close connection with database."""
        self.cursor.close()
        self.conn.close()

    def create_table(self, table):
        """Create a new table in database."""
        self.connect()

        if type(table) is DBTable:
            create = table.query_create()
        elif type(table) is str:
            create = schema_create(table)
        else:
            raise Exception('Must be DBTable or in schema file')

        self.cursor.execute(create)
        self.close()

    def execute_commit(self, query):
        """Execute a query and commit any changes to database."""
        try:
            self.cursor.execute(query)
            self.conn.commit()
        except pymysql.err.DataError:
            print '{} not executed'.format(query)

    def insert(self, name, data, at_once=True):
        """Insert new rows to database."""
        self.connect()
        
        table = make_table(name, data)

        # insert full table at once
        if at_once == True:
            insert_full = table.query_insert(at_once=at_once)
            self.cursor.execute(insert_full)
            self.conn.commit()
        # insert one row at a time
        else:
            insert_rows = table.query_insert(at_once=at_once)
            map(lambda x: self.execute_commit(x), insert_rows)

        self.close()

    def create_insert(self, name, data, at_once=True):
        """Create new table and insert new rows in database."""
        table = make_table(name, data)
        self.create_table(table)
        self.insert(name, table, at_once=at_once)

    def delete(self, table):
        """Delete all rows in a table in database."""
        self.connect()
        query = "DELETE from {}".format(table.name)
        self.cursor.execute(query)
        self.conn.commit()
        self.close()

    def replace(self, name, data, at_once=True):
        """Replace all rows in a table in database."""
        self.connect()

        table = make_table(name, data)

        self.delete(table)
        self.insert(table, at_once=at_once)

        self.close()

    def run_query(self, query, as_dict=True):
        """Return the results of a query on database."""
        self.connect()
        if as_dict == True:
            self.cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        self.close()
        return result

    def table_columns(self, table_name):
        """Return the column names from table in database."""
        query = """SHOW COLUMNS FROM %s;""" % (table_name)
        result = self.run_query(query, as_dict=False)
        column_names = [col[0] for col in result]
        return column_names  

    def return_data(self, table_name, as_list=False, subset=None, modifier=""):
        """Return the data from table in database."""
        if subset is None:
            get = '*'
        elif type(subset) is str:
            get = column
        elif type(subset) is list:
            get = ", ".join(column)
        else:
            sub_type = type(subset)
            msg = 'Subset must be None, str, or list, not {}'.format(sub_type)
            raise Exception(msg)

        # list of column indices where type is decimal
        query = "SELECT {0} FROM {1} {2};".format(get, table_name, modifier)
        result = self.run_query(query)
        df = pd.DataFrame(result)
        
        if as_list is True:
            return df_list(df)
        else:
            return df


def schema_create(table_name, schema_file=src.constants.SCHEMA_FILE):
    """Return the string used as a query to create a table."""
    with open(schema_file, 'r') as f:
        schema = json.load(f)[table_name]
    cols = ["{0} {1}".format(c['name'], c['type']) for c in schema]
    col_part = ", \n".join(cols)
    query_create = "CREATE TABLE {0} ({1});".format(table_name, col_part)
    return query_create


def make_table(name, data):
    """Return a DBTable instance from a nested list or pandas DataFrame."""
    if type(data) is DBTable:
        table = data
    elif type(data) is list:
        table = DBTable(name, data)
    elif type(data) is pd.core.frame.DataFrame:
        rows = df_list(data)
        table = DBTable(name, rows)
    else:
        raise Exception('data must be list or DataFrame')

    return table


def df_list(df):
    """Return DataFrame columns and values as a nested list."""
    col_names = list(df.columns)
    data = df.values.tolist()
    data.insert(0, col_names)
    return data
