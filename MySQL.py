import os
import pymysql
import logging
import pandas as pd
from data_interface import DataInterface
from dotenv import find_dotenv, load_dotenv
from log_config import AzureTableStorageHandler


path = find_dotenv()
load_dotenv(path)
CONNECTION_STRING = os.getenv("CONNECTION_STRING")
AZURE_TABLE_NAME = os.getenv("TABLE_NAME")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
azure_handler = AzureTableStorageHandler(connection_string=CONNECTION_STRING, table_name=AZURE_TABLE_NAME)
logger.addHandler(azure_handler) # Log into Azure Table
logger.addHandler(logging.StreamHandler()) # Log into command-line console
# logging.basicConfig(level=logging.INFO)

class MySQL(DataInterface):
    '''
    :param host: Host where the database server is located.
    :param user: Username to log in as.
    :param password: Password to use.
    :param database: Database to use, None to not use a particular one.
    '''
    def __init__(self,
                 database=None, 
                 table_name=None, 
                 columns_datatype=None, 
                 query=None, 
                 columns_header=None,
                 host=None, 
                 user=None, 
                 password=None):
        self.connection_params = {'host': host, 'user': user, 'password': password, 'db': database}
        self.connection_string = f"mysql+pymysql://{user}:{password}@{host}/{database}"
        self.table_name = table_name
        self.columns_datatype = columns_datatype
        super().__init__(connection_params=self.connection_params, connection_string=self.connection_string, query=query, columns_header=columns_header)
        self.connect_to_database()
    
    def connect_to_database(self):
        self.connect = pymysql.connect(**self.connection_params)

    def create_table(self, table_name, columns_datatype):
        cursor = self.connect.cursor()
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_datatype)})"
        logger.info(f"Executing: {query}")
        cursor.execute(query)
        self.connect.commit()
    
    def write_directly(self, table_name, data, number_of_columns):
        cursor = self.connect.cursor()
        str = '%s'
        for i in range(number_of_columns-1):
            str = str + ", %s"
        query = f"INSERT INTO {table_name} VALUES ({str})"
        cursor.executemany(query, data)
        logger.info(f"Insert Data Successfully")
        self.connect.commit()

    def write(self, file_path):
        data = pd.read_csv(file_path)
        self.write_to_database(table_name=self.table_name, data=data)
