import os
import pyodbc
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

class SQL_Server(DataInterface):
    '''
    :param server: specific SQL Server.
    :param driver: specific SQL Server's Driver.
    :param database: Database to use, None to not use a particular one.
    '''
    def __init__(self, 
                 server=None, 
                 driver=None, 
                 database=None, 
                 table_name=None, 
                 columns_datatype=None, 
                 query=None, 
                 columns_header=None):
        self.connection_params = f"DRIVER=SQL Server;" f"SERVER={server};" f"DATABASE={database}"
        self.connection_string = f"mssql+pyodbc://@{server}/{database}?driver={driver}"
        self.table_name = table_name
        self.columns_datatype = columns_datatype
        super().__init__(connection_params=self.connection_params, connection_string=self.connection_string, query=query, columns_header=columns_header)
        self.connect_to_database()

    def connect_to_database(self):
        self.connect = pyodbc.connect(self.connection_params)

    def create_table(self, table_name, columns_datatype):
        cursor = self.connect.cursor()
        query = f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}') BEGIN CREATE TABLE dbo.{table_name} ({', '.join(columns_datatype)}); END;"
        logger.info(f"Executing: {query}")
        cursor.execute(query)
        self.connect.commit()
    
    def write_directly(self, table_name, data, number_of_columns):
        cursor = self.connect.cursor()
        str = '?'
        for i in range(number_of_columns-1):
            str = str + ", ?"
        query = f"INSERT INTO {table_name} VALUES ({str})"
        cursor.executemany(query, data)
        logger.info(f"Insert Data Successfully")
        self.connect.commit()
    

    def write(self, file_path):
        data = pd.read_csv(file_path)
        self.write_to_database(table_name=self.table_name, data=data)
        
