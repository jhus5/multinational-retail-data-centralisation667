#import libraries
import yaml
from sqlalchemy import create_engine
from data_cleaning import DataCleaning

class DatabaseConnector:
    def __init__(self):
        self.db_connector = self.init_local_db_connector()
    # Reading creddentials for local data (seperate file from cloud db)
    def read_local_db_creds(self, file_path='local_db_creds.yaml'):
        try:
            with open(file_path, 'r') as file:
                credentials = yaml.safe_load(file)
            return credentials
        except FileNotFoundError:
            print(f"Error: The file {file_path} was not found.")
        except yaml.YAMLError as e:
            print(f"Error reading YAML file: {e}")
        return None

    # Connecting to a local PostgreSQL database
    def init_local_db_connector(self):
        credentials = self.read_db_creds()
        if credentials:
            db_url = f"postgresql://{credentials.get('RDS_USER', '')}:{credentials.get('RDS_PASSWORD', '')}@localhost:5432/{credentials.get('RDS_DATABASE', '')}"
            
            # Initialize and return the DatabaseConnector
            return DatabaseConnector(db_url)
        else:
            return None

    def upload_to_db(self, cleaned_data, table_name='legacy_users'):
        if self.db_connector:
            self.db_connector.upload_to_db(cleaned_data, table_name)
        else:
            print("Error: Database connector not initialized.")
