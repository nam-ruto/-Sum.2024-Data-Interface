import os
import logging
import pandas as pd
from data_interface import DataInterface
from dotenv import find_dotenv, load_dotenv
from log.log_config import AzureTableStorageHandler



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

class csv(DataInterface):
    def __init__(self, db_type, connection_params=None):
        super().__init__(db_type, connection_params)
    
    def read(self, file_path):
        self.data_frame = pd.read_csv(file_path)
        logger.info("Read from csv successfully")
        return self.data_frame

    def write(self, file_path):
        self.data_frame.to_csv(file_path, index=False)
        logger.info("Write to csv successfully")