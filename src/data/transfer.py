""" transfer

A module for data insertion and extraction with MySQL database.

Classes
-------
DBColumn
    A container for one column of data in a MySQL table.
DBTable
    A container of data for a MySQL table.
DBAssist
    A tool for creating MySQL objects and extracting data from database.

"""
import ConfigParser
import json
import pandas as pd
import pymysql
from src.constants import SCHEMA_FILE
from src.constants import CONFIG_FILE
from src.constants import DB_NAME


class DBAssist():
    """
    A tool for creating MySQL objects and extracting data from database.

    Attributes
    ----------
    db_name : str
        Name of the target MySQL database.
    conn : pymysql.connection
        A pymysql connection with MySQL database.
    cursor : pymysql.cursors.Cursor
        A pymysql cursor called from the conn attribute.

    """
    def __init__(self, db_name=DB_NAME):
        """Initialize DBAssist instance."""
        self.db_name = db_name
        self.conn = self.connect()
        self.cursor = self.conn.cursor()

    def connect(self, config_file=CONFIG_FILE):
        """
        Establish connection with database.
        
        Uses an INI-formatted configuration file for database 
        access settings. Settings for the target database should be listed
        under some label with 'user' and 'pwd' like so:
        
        [generic_label]
        user = username
        pwd = 1234
        
        Parameters
        ----------
        config_file : str
            Path to file INI file with user name and password for database.

        Returns
        -------
        conn : pymysql.connections.Connection
            An open connection with MySQL database.

        """
        parser = ConfigParser.ConfigParser()
        parser.readfp(open(config_file))
        
        conn = pymysql.connect(host='127.0.0.1',
                               port=3306,
                               user=parser.get('Local', 'user'),
                               passwd=parser.get('Local', 'pwd'),
                               db=self.db_name)
        return conn

    def close(self):
        """Close cursor and connection with database."""
        self.cursor.close()
        self.conn.close()

    def create_from_data(self, table_name, data):
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

    def create_table(self, query_create):
        """
        Execute query used to create a table.

        Parameters
        ----------
        query_create : str
            
        """
        try:
            self.cursor.execute(query_create)
        except pymysql.err.InternalError, e:
            print e

    def schema_query(self, table_name, schema_file=SCHEMA_FILE):
        """
        Return query used to create MySQL table using the specified schema.

        Parameters
        ----------
        table_name : str
            Name of the table as specified in the json file.
        schema_file : str
            Location of json file with table specs. Default is imported from
            src/constants.py.

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

    def execute_commit(self, query):
        """
        Execute a query and commit changes to database.

        Helpful to iterate over lists of insert queries to keep errors from 
        killing the script.

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

        # insert full table at once
        if at_once == True:
            query_one = table.query_insert(at_once=at_once)
            self.execute_commit(query_one)
        # insert one row at a time
        else:
            query_list = table.query_insert(at_once=at_once)
            map(lambda x: self.execute_commit(x), query_list)

    def delete_rows(self, table_name):
        """
        Delete all rows in a table in database.

        Parameters
        ----------
        table_name : str
            Name of the table in database.

        """
        query_delete = "DELETE from {}".format(table_name)
        self.execute_commit(query_delete)

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
        # set SELECT objects according to subset parameter
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
        query = "SELECT {} FROM {} {};".format(columns_to_get, table_name,
                                                  modifier)
        result = self.query_result(query)
        df = pd.DataFrame(result)

        return df

    def query_result(self, query, as_dict=True):
        """
        Return the results of a query on database.

        Parameters
        ----------
        query : str
            Text of a valid MySQL query.
        as_dict : bool, default True
            Return list of dict with value labels as keys for each dict.
            If False, return tuple of tuples with tuple elements in order of
            elements in table.

        Returns
        -------
        result : list of dict, tuple of tuple
            Result of the query.

        """
        # new cursor object to prevent altering the instance cursor attribute
        if as_dict == True:
            cursor = self.conn.cursor(pymysql.cursors.DictCursor)
        else:
            cursor = self.conn.cursor()
        
        cursor.execute(query)
        return cursor.fetchall()

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


class DBTable(object):
    """
    A container for tabular data for insertion to MySQL table.

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
        self.data = self.verify_data(data)
        # initialize all columns to set names and types
        # data transposed to list of columns to extract column types
        self.columns = [DBColumn(x) for x in map(list, zip(*self.data))]

    def verify_data(self, data):
        """
        Verity data attribute meets input requirements.
        
        Parameters
        ----------
        data : list of list or pandas DataFrame
            The original cell value when DBTable is initialized.

        Returns
        -------
        data_list : list of list
            First list element is list of column names. Remaining elements are
            lists of values for table rows.

        """
        if type(data) is list:
            data_list = data
        elif type(data) is pd.core.frame.DataFrame:
            col_names = list(data.columns)
            data_list = data.values.tolist()
            data_list.insert(0, col_names)
        else:
            raise Exception('data must be list or DataFrame')
        
        column_names = data_list[0]
        name_types = list(set([type(x).__name__ for x in column_names]))
        if name_types != ['str']:
            raise Exception('all column names must be str type')

        return data_list

    def query_create(self):
        """Return text of query to create table in MySQL database."""
        # obtain column portion of query with name and type
        query_columns = ", \n".join([c.name_type for c in self.columns])
        create = "CREATE TABLE {} ({});".format(self.name, query_columns)
        return create

    def query_insert(self, at_once=True):
        """
        Return query for inserting data to table in MySQL database.
        
        Parameters
        ----------
        at_once : bool, default True
            Return a single insert query for inserting all rows at once.
            If False, return a list with separate insert query for each row.

        Returns
        -------
        insert : str or list of str
            Str or list for inserting table values to MySQL database.

        """
        # create prefix string for insert query
        col_string = ", ".join(self.data[0])
        pref = "INSERT INTO {0} ({1}) VALUES".format(self.name, col_string)
        
        # list with formatted string for each row
        table_rows = self.rows_for_insert()
        
        if at_once is True:
            insert = "{0}\n{1};".format(pref, ",\n".join(table_rows))
        else:
            insert = ["{0} \n{1};".format(pref, row) for row in table_rows]

        return insert

    def rows_for_insert(self):
        """Return list of str containing row values for table."""
        # convert all values to str objects for insert query text
        # list of columns to keep column types consistent
        column_values = [c.convert_column() for c in self.columns]
        # transpose back to list of row lists
        rows = map(list, zip(*column_values))
        rows_joined = ["({0})".format(", ".join(row)) for row in rows]
        #rows_closed = ["({0})".format(row) for row in rows_joined]
        return rows_joined


class DBColumn(object):
    """
    A container for one column of data in a MySQL table.

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
        self.type = self.return_type()
        self.name_type = "".join([self.name, self.type])

    def return_type(self):
        """Return string indicating type to use for column."""
        # collect all unique types as strings
        elem_types = list(set([type(x).__name__ for x in self.values]))

        # if any strings use VARCHAR, otherwise use FLOAT
        if 'str' in elem_types:
            # len 64 is minimumm sufficient size for expected values
            column_type = " VARCHAR (64)"
        else:
            column_type = " FLOAT"

        return column_type

    def convert_column(self):
        """
        Return list of column values converted for MySQL insertion.
        
        To create the text for an insert query, the column's raw values are 
        converted to strings and missing values are converted to NULL.

        Returns
        -------
        new_values : list of str
            String values converted from numeric or string types. 
        
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
            # because string types enclosed in '', string literal for single '
            new_value = str(raw_value).replace("'", r"\'")
            new_value = r"""'{}'""".format(new_value)
        else:
            new_value = """{}""".format(raw_value)

        return new_value
