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
    SERVER = sys.argv[5]
    DATABASE = sys.argv[6]
    DRIVER = sys.argv[7]
    COLUMN_STRING = sys.argv[8]
    COLUMN_DATATYPE=COLUMN_STRING.split(',')
    CONNECTION_PARAMS = (f"DRIVER=SQL Server;" f"SERVER={SERVER};" f"DATABASE={DATABASE}")
    CONNECTION_STRING = f"mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}"
    TABLE_NAME = sys.argv[9]
    # COLUMN_HEADER=sys.argv[10]


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


    # # Create database object, create table, write data
    mssql = data_interface.sql_server(db_type='SQL Server', connection_params=CONNECTION_PARAMS)
    mssql.connect_to_database()
    mssql.create_table(table_name=TABLE_NAME, columns_datatype=COLUMN_DATATYPE)
    mssql.write_to_database(conn_string=CONNECTION_STRING, data=df, list_name=TABLE_NAME)
    


    # Run Python with argms
    # create sh file
    # Write --> csv --> azure storage
    # naming format: name_yyyymmdd.csv
