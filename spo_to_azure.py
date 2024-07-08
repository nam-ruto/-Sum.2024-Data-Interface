import os
import sys
import pandas as pd
import data_interface
from dotenv import find_dotenv, load_dotenv
path = find_dotenv()
load_dotenv(path)


if __name__ == "__main__":
    # Input parameter from SharePoint List: user_name, password, site_url, list_title
    USER_NAME = os.getenv("USER_NAME")
    PASSWORD = os.getenv("PASSWORD")
    LIST_NAME = sys.argv[1]
    FIELD_STRING = sys.argv[2]
    SITE_URL = sys.argv[3]
    BASE_URL = sys.argv[4]
    FIELD_NAME = FIELD_STRING.split(',')
    SELECT_QUERY = f"$select={FIELD_STRING}"


    # Output parameter --> Destination storage
    FILE_PATH = sys.argv[5] # CSV file
    ACCOUNT_URL = sys.argv[6]
    CONTAINER_NAME = sys.argv[7]
    BLOB_NAME = sys.argv[8]


    # Create an SharePoint object
    mySharePoint = data_interface.SharePoint(user_name=USER_NAME, password=PASSWORD, site_url=SITE_URL, base_url=BASE_URL)
    

    # Authenticate
    mySharePoint.user_authenticate()


    # Retrieve data from List
    data = mySharePoint.get_list_item(list_name=LIST_NAME, select_query=SELECT_QUERY)
    field_metadata = mySharePoint.get_field_metadata(list_name=LIST_NAME, field_names=FIELD_NAME)
    result = mySharePoint.mapping(data=data, field_metadata=field_metadata)
    df = pd.DataFrame(data=result)
    print(df)


    # Write to local csv file
    df.to_csv(path_or_buf=FILE_PATH)


    # Blob Storage
    blob = data_interface.Blob(container_name=CONTAINER_NAME)
    blob.blob_credential(account_url=ACCOUNT_URL)
    blob.upload_blob(container_name=CONTAINER_NAME, blob_name=BLOB_NAME, file_path=FILE_PATH)
