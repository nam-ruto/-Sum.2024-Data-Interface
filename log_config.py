import logging
from azure.data.tables import TableServiceClient
from datetime import datetime

class AzureTableStorageHandler(logging.Handler):
    def __init__(self, connection_string, table_name):
        super().__init__()
        self.table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string)
        self.table_client = self.table_service_client.get_table_client(table_name=table_name)
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        try:
            self.table_client.create_table()
        except Exception as e:
            if "TableAlreadyExists" not in str(e):
                print(f"Table creation error: {e}")

    def emit(self, record):
        try:
            # Avoid logging errors that occur in this handler
            if record.name == self.__class__.__name__:
                return
            log_entry = self.format(record)
            entity = {
                'PartitionKey': record.levelname,
                'RowKey': f"{record.created}-{record.levelname}-{record.name}",
                'LogLevel': record.levelname,
                'Message': record.getMessage(),  # Use getMessage() to get the formatted message
                'LoggerName': record.name,
                'CreatedTime': datetime.fromtimestamp(record.created).isoformat(),
            }
            self.table_client.create_entity(entity=entity)
        except Exception as e:
            if "EntityAlreadyExists" not in str(e):
                # Only print the error if it's not due to duplicate entries
                print(f"Logging error: {e}")

class CustomFormatter(logging.Formatter):
    def format(self, record):
        return f"{datetime.fromtimestamp(record.created).isoformat()} - {record.name} - {record.levelname} - {record.getMessage()}"