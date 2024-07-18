import os
import logging
import pandas as pd
from sqlalchemy import create_engine 
from dotenv import find_dotenv, load_dotenv
from log_config import AzureTableStorageHandler


# Config logging - Logs will be written into Azure Table Storage
path = find_dotenv()
load_dotenv(path)
CONN_STRING = os.getenv("CONNECTION_STRING")
AZURE_TABLE_NAME = os.getenv("TABLE_NAME")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
azure_handler = AzureTableStorageHandler(connection_string=CONN_STRING, table_name=AZURE_TABLE_NAME)
logger.addHandler(azure_handler) # Log into Azure Table
logger.addHandler(logging.StreamHandler()) # Log into command-line console
# logging.basicConfig(level=logging.INFO)


class DataInterface:
    def __init__(self, connection_params=None, connection_string=None, query=None, columns_header=None):
        self.connection_params = connection_params
        self.connection_string = connection_string
        self.query = query
        self.columns_header = columns_header
        self.connect = None
        self.data_frame = None

    def connect_to_database():
        pass

    # Common method for MySQL and SQL Server 
    def read(self, file_path):
        cursor = self.connect.cursor()
        logger.info(f"Executing: {self.query}")
        cursor.execute(self.query)
        rows = cursor.fetchall()
        data = [tuple(row) for row in rows]
        df = pd.DataFrame(data=data, columns=self.columns_header)
        df.to_csv(file_path, index=False)
        return df, rows

    def write_to_database(self, data=None, table_name=None):
        engine = create_engine(self.connection_string)
        con = engine.connect()
        data.to_sql(name=table_name, con=con, if_exists='replace', index=False)
        logger.info(f"Data is written to MySQL table '{table_name}' successfully.")

    def to_csv(self, data, file_path):
        with open(file=file_path, mode='w', newline='') as file:
            file.write(data)
    
    def close(self):
        if self.connect:
            self.connect.close()
