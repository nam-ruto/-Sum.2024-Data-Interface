#!/bin/bash

# Read Data from SharePoint List
# Save as local csv file
# Write to Azure Blob Storage

# SharePoint parameters
LIST_NAME="Employee"
FIELD_STRING="Title,field_1,field_2,field_3,field_4"
SITE_URL="https://husteduvn.sharepoint.com/sites/PhngCaNam/"
BASE_URL="https://husteduvn.sharepoint.com/sites/PhngCaNam/_api/"


# Destination storage paramaters
SERVER="DESKTOP-I5TALT9\\NAMRUTO"
DATABASE="my_database"
DRIVER="ODBC Driver 17 for SQL Server"
COLUMN_STRING="education VARCHAR(50),joiningYear INT,city VARCHAR(50),paymenttier INT,age INT"
TABLE_NAME="spo_copy1"
COLUMN_HEADER=""


python main.py "$LIST_NAME" "$FIELD_STRING" "$SITE_URL" "$BASE_URL" "$SERVER" "$DATABASE" "$DRIVER" "$COLUMN_STRING" "$TABLE_NAME"