#!/bin/sh

# SharePoint List Parameter
LIST_NAME="superstore_sales"
FIELD_STRING="Title,field_1,field_2,field_3,field_4,field_5,field_6,field_7,field_8,field_9,field_10,field_11,field_12,field_13,field_14,field_15,field_16,field_17"
SITE_URL="https://husteduvn.sharepoint.com/sites/HDN-Lab/"
BASE_URL="https://husteduvn.sharepoint.com/sites/HDN-Lab/_api"


# CSV file path
current_date=$(date +%Y%m%d)
prefix_name="superstore_sales_copy"
folder="./data/"
FILE_PATH="$folder""$prefix_name"_"$current_date".csv


# Azure Blob Storage
ACCOUNT_URL="https://hdnthanos.blob.core.windows.net/"
CONTAINER_NAME="hdn-album"
BLOB_NAME="$prefix_name"_"$current_date".csv


# Command
python spo_to_azure.py "$LIST_NAME" "$FIELD_STRING" "$SITE_URL" "$BASE_URL" "$FILE_PATH" "$ACCOUNT_URL" "$CONTAINER_NAME" "$BLOB_NAME"