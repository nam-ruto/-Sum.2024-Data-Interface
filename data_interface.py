import os
import json
import pyodbc
import pymysql
import logging
import pandas as pd
from sqlalchemy import create_engine 
from dotenv import find_dotenv, load_dotenv
from log_config import AzureTableStorageHandler
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from office365.sharepoint.request import SharePointRequest
from office365.sharepoint.client_context import ClientCredential
from office365.runtime.auth.user_credential import UserCredential


# Config logging - Logs will be written into Azure Table Storage
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


class DataInterface:
    def __init__(self, db_type, connection_params=None):
        self.db_type = db_type
        self.connection_params = connection_params
        self.connect = None
        self.data_frame = None

    def connect_to_database():
        pass

    # Common method for MySQL and SQL Server 
    def read(self, query=None, columns_header=None):
        cursor = self.connect.cursor()
        logger.info(f"Executing: {query}")
        cursor.execute(query)
        rows = cursor.fetchall()
        data = [tuple(row) for row in rows]
        df = pd.DataFrame(data=data, columns=columns_header)
        return df, rows

    def write_to_database(self, conn_string=None, data=None, list_name=None):
        engine = create_engine(conn_string)
        con = engine.connect()
        data.to_sql(name=list_name, con=con, if_exists='replace', index=False)
        logger.info(f"Data written to MySQL table '{list_name}' successfully.")

    def to_csv(self, data, file_path):
        with open(file=file_path, mode='w', newline='') as file:
            file.write(data)
    
    def close(self):
        if self.connect:
            self.connect.close()

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

class mysql(DataInterface):
    def __init__(self, db_type, connection_params=None):
        super().__init__(db_type, connection_params)
    
    def connect_to_database(self):
        self.connect = pymysql.connect(**self.connection_params)

    def create_table(self, table_name, columns_datatype):
        cursor = self.connect.cursor()
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns_datatype)})"
        logger.info(f"Executing: {query}")
        cursor.execute(query)
        self.connect.commit()
    
    def write(self, table_name, data, number_of_columns):
        cursor = self.connect.cursor()
        str = '%s'
        for i in range(number_of_columns-1):
            str = str + ", %s"
        query = f"INSERT INTO {table_name} VALUES ({str})"
        cursor.executemany(query, data)
        logger.info(f"Insert Data Successfully")
        self.connect.commit()

class sql_server(DataInterface):
    def __init__(self, db_type, connection_params=None):
        super().__init__(db_type, connection_params)

    def connect_to_database(self):
        self.connect = pyodbc.connect(self.connection_params)

    def create_table(self, table_name, columns_datatype):
        cursor = self.connect.cursor()
        query = f"IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = '{table_name}') BEGIN CREATE TABLE dbo.{table_name} ({', '.join(columns_datatype)}); END;"
        logger.info(f"Executing: {query}")
        cursor.execute(query)
        self.connect.commit()
    
    def write(self, table_name, data, number_of_columns):
        cursor = self.connect.cursor()
        str = '?'
        for i in range(number_of_columns-1):
            str = str + ", ?"
        query = f"INSERT INTO {table_name} VALUES ({str})"
        cursor.executemany(query, data)
        logger.info(f"Insert Data Successfully")
        self.connect.commit()
    
class Blob(DataInterface):
    def __init__(self, container_name=None, blob_name=None):
        self.container_name = container_name
        self.blob_name = blob_name
        self.blob_service_client = None
    
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
    
    def upload_blob(self, container_name, blob_name, file_path):
        blob_client = self.blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        with open(file=file_path, mode="rb") as data:
            blob_client.upload_blob(data=data)
            logger.info("Upload blob successfully")

class SharePoint(DataInterface):
    def __init__(self, request=None, user_name=None, password=None, site_url=None, base_url=None, client_id=None, tenant_id=None, client_secret=None):
        self.request = request
        self.user_name = user_name
        self.password = password
        self.site_url = site_url
        self.base_url = base_url
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.client_secret = client_secret

    # Service Principle Authentication
    def client_authenticate(self):
        try:
            self.request = SharePointRequest(self.site_url).with_credentials(ClientCredential(client_id=self.client_id, client_secret=self.client_secret))
            test_response = self.request.execute_request("web")
            if test_response.status_code == 200:
                logger.info("Authenticated using user credentials successfully.")
            else:
                logger.error(f"Authentication failed using user credentials. Status code: {test_response.status_code}")
        except Exception as e:
            logger.error(f"Failed to authenticate using user credentials: {e}") 

    # User Credentials Authentication
    def user_authenticate(self):
        try:
            self.request = SharePointRequest(self.site_url).with_credentials(UserCredential(self.user_name, self.password))
            # Perform a test request to verify authentication
            test_response = self.request.execute_request("web")
            if test_response.status_code == 200:
                logger.info("Authenticated using user credentials successfully.")
            else:
                logger.error(f"Authentication failed using user credentials. Status code: {test_response.status_code}")
        except Exception as e:
            logger.error(f"Failed to authenticate using user credentials: {e}")

    # Get list data with the internal columns --> Return a list of dictionary
    def get_list_item(self, list_name, select_query):
        all_data = []
        endpoint = f"web/lists/getbytitle('{list_name}')/items?{select_query}"
        while endpoint:
            response = self.request.execute_request(endpoint)
            data = json.loads(response.content)
            data = response.json()
            all_data.extend(data['d']['results'])
            next_page = data['d'].get('__next')
            if next_page:
                # Ensure the next page URL is correctly formatted
                endpoint = next_page.replace(self.base_url, "")
            else:
                endpoint = None
        return all_data

    # Get external name (display name) of column --> Return a dictionary
    def get_field_metadata(self, list_name=None, field_names: list = None):
        fields_metadata = {} # Contain pair of key: value (internal name: external name)

        field_filters = ' or '.join([f"InternalName eq '{field}'" for field in field_names])
        endpoint = f"web/lists/getbytitle('{list_name}')/fields?$filter={field_filters}"
        response = self.request.execute_request(endpoint)
        data = json.loads(response.content)
        data = response.json()
        fields = data['d']['results']
        for field in fields:
            if field_names is None or field['InternalName'] in field_names:
                fields_metadata[field['InternalName']] = field['Title']
        return fields_metadata
    
    # Mapping data with the external name (Display Name)
    def mapping(self, data: list, field_metadata: dict):
        transformed_data = []
        for item in data:
            transformed_item = {}
            for internal, external in field_metadata.items():
                transformed_item[external] = item.get(internal, None) 
            transformed_data.append(transformed_item)
        return transformed_data