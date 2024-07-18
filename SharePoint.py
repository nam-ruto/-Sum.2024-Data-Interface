from data_interface import DataInterface
import logging
import json
import os
import pandas as pd
from dotenv import find_dotenv, load_dotenv
from office365.sharepoint.request import SharePointRequest
from office365.sharepoint.client_context import ClientCredential
from office365.runtime.auth.user_credential import UserCredential
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

class SharePoint(DataInterface):
    def __init__(self, request=None, user_name=None, password=None, site_url=None, base_url=None, client_id=None, tenant_id=None, client_secret=None, cre_type=None,
                 list_name=None, field_string:str=None):
        self.request = request
        self.site_url = site_url
        self.base_url = base_url

        # Parameter for User Authenticate
        self.user_name = user_name
        self.password = password

        # Parameter for Service Principle
        self.client_id = client_id
        self.tenant_id = tenant_id
        self.client_secret = client_secret

        # Credential Type
        self.cre_type = cre_type

        # Query Information
        self.list_name = list_name
        self.field_names = field_string.split(',')
        self.select_query = f"$select={field_string}"

        # File path
        self.file_name = f"{list_name}.csv"

        # Check credential to authenticate
        if(self.cre_type == "service_principle"):
            self.client_authenticate()
        else:
            self.user_authenticate()


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
    def get_list_item(self):
        all_data = []
        endpoint = f"web/lists/getbytitle('{self.list_name}')/items?{self.select_query}"
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
    def get_field_metadata(self):
        fields_metadata = {} # Contain pair of key: value (internal name: external name)

        field_filters = ' or '.join([f"InternalName eq '{field}'" for field in self.field_names])
        endpoint = f"web/lists/getbytitle('{self.list_name}')/fields?$filter={field_filters}"
        response = self.request.execute_request(endpoint)
        data = json.loads(response.content)
        data = response.json()
        fields = data['d']['results']
        for field in fields:
            if self.field_names is None or field['InternalName'] in self.field_names:
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
    
    def read(self, file_path):
        data = self.get_list_item()
        field_metadata = self.get_field_metadata()
        result = self.mapping(data=data, field_metadata=field_metadata)
        df = pd.DataFrame(result)
        print(df)
        df.to_csv(file_path, index=False)