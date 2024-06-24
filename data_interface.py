import os
import pyodbc
import pymysql
import logging
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from log_config import AzureTableStorageHandler
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from office365.sharepoint.listitems.caml import query
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.authentication_context import AuthenticationContext

# Config logging - Logs will be written into Azure Table Storage
path = find_dotenv()
load_dotenv(path)
CONNECTION_STRING = os.getenv("CONNECTION_STRING")
TABLE_NAME = os.getenv("TABLE_NAME")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
azure_handler = AzureTableStorageHandler(connection_string=CONNECTION_STRING, table_name=TABLE_NAME)
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
    def read(self, query, columns_header):
        cursor = self.connect.cursor()
        logger.info(f"Executing: {query}")
        cursor.execute(query)
        rows = cursor.fetchall()
        data = [tuple(row) for row in rows]
        df = pd.DataFrame(data=data, columns=columns_header)
        return df, rows

    def write():
        pass

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
    def __init__(self, container_name, blob_name):
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

class SharePoint(DataInterface):
    def __init__(self, user_name=None, password=None, site_url=None, client_id=None, tenant_id=None, client_secret=None):
        self.user_name = user_name
        self.password = password
        self.site_url = site_url
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.client_secret = client_secret
        self.context = None
    
    def app_only_auth():
        pass

    def user_delegation_auth():
        pass

    def user_cre_auth(self):
        ctx_auth = AuthenticationContext(self.site_url)
        if ctx_auth.acquire_token_for_user(username=self.user_name, password=self.password):
            logger.info("Authenticate successfully")
            self.context = ClientContext(base_url=self.site_url, auth_context=ctx_auth)
        else:
            logger.error(ctx_auth.get_last_error())
    
    def get_site_title(self):
        web = self.context.web
        self.context.load(web)
        self.context.execute_query()
        print(f"Site title: {web.properties['Title']}")
    
    def get_list_data(self, list_title):
        # Retrieve Web Properties
        web = self.context.web

        # Retrieve specific list
        sp_list = web.lists.get_by_title(list_title=list_title)
        self.context.load(sp_list)
        self.context.execute_query()
        print(f"List title: {sp_list.properties['Title']}")

        # Retrieve list's field
        fields = sp_list.fields
        self.context.load(fields)
        self.context.execute_query()
        field_mapping = {field.properties['InternalName']: field.properties['Title'] for field in fields}

        # Retrieve list's data
        caml_query = query.CamlQuery(view_xml=True)
        caml_query.ViewXml = "<View><Query></Query></View>"
        items = sp_list.get_items(caml_query) # It will return dictionaries with internal column and correspond data
        self.context.load(items)
        self.context.execute_query()

        data = []
        for item in items:
            data.append(item.properties)
        df = pd.DataFrame(data=data)
        selected_columns = ['Title'] + [col for col in df.columns if col.startswith('field_')] # Take only necessary column
        self.data_frame = df[selected_columns].copy()
        self.data_frame.rename(columns=field_mapping, inplace=True) # Remove internal column name with its actual name

        # Convert to dataframe to tuple
        final_data = tuple(tuple(row) for row in self.data_frame.itertuples(index=False, name=None))

        return self.data_frame, final_data
    
    def to_csv(self, file_path):
        self.data_frame.to_csv(file_path=file_path, index=False)