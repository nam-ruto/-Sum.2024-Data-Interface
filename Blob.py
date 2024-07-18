import os
import logging
import datetime
from data_interface import DataInterface
from dotenv import find_dotenv, load_dotenv
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
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

class Blob(DataInterface):
    def __init__(self, container_name=None, blob_name=None, account_url=None):
        self.container_name = container_name
        self.blob_name = blob_name
        self.blob_service_client = None
        self.blob_credential(account_url=account_url)
    
    def blob_credential(self, account_url):
        credentials = DefaultAzureCredential()
        self.blob_service_client = BlobServiceClient(account_url=account_url, credential=credentials)
    
    def get_blob_data(self):
        try:
            logger.info(f"BlobServiceClient created: {self.blob_service_client}")

            # Get the container client
            container_client = self.blob_service_client.get_container_client(container=self.container_name)

            # Get the blob client
            blob_client = container_client.get_blob_client(blob=self.blob_name)

            # Download the blob data
            blob_data = blob_client.download_blob().readall().decode("utf-8")
            logger.info(f"Blob data downloaded successfully: {self.blob_name}")

            return blob_data
        except Exception as e:
            logger.error(f"An error occurred: {e}")
    
    def get_blob_list(self):
        container_client = self.blob_service_client.get_container_client(container=self.container_name)
        blob_list = container_client.list_blobs()
        for blob in blob_list:
            print(f"Name: {blob.name}")
            print(f"  Size: {blob.size} bytes")
            print(f"  Last modified: {blob.last_modified}")
            print(f"  Blob type: {blob.blob_type}")
            print(f"  ETag: {blob.etag}")
            print(f"  Content type: {blob.content_settings.content_type}")
        
    def naming_blob(self):
        current_time = datetime.datetime.now().strftime('%Y%m%d_%H:%M:%S')
        self.blob_name = f"{self.blob_name}_{current_time}.csv"
    
    def write(self, file_path):
        self.naming_blob()
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=self.blob_name)
        with open(file=file_path, mode="rb") as data:
            blob_client.upload_blob(data=data)
            logger.info("Upload blob successfully")