import yaml
import pandas as pd
from sqlalchemy import create_engine
#from sqlalchemy_utils import database_exists, create_database
from data_cleaning import DataCleaning

class DatabaseConnector:
    def __init__(self, db_url=None):
        self.db_engine = create_engine(db_url) if db_url else None

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

    def init_local_db_connector(self):
        credentials = self.read_local_db_creds()
        if credentials:
            # Change the database URL to connect to a local PostgreSQL database
            db_url = f"postgresql://{credentials.get('RDS_USER', '')}:{credentials.get('RDS_PASSWORD', '')}@localhost:5432/{credentials.get('RDS_DATABASE', '')}"
            
            # Initialize and return the DatabaseConnector with db_url
            return DatabaseConnector(db_url)
        else:
            return None

    def upload_to_db(self, cleaned_data, table_name='dim_users'):
        try:
            # Convert DataFrame to SQL and upload to the specified table
            cleaned_data.to_sql(name=table_name, con=self.db_engine, if_exists='replace', index=False)
            print(f"Data uploaded to the '{table_name}' table successfully.")
        except Exception as e:
            print(f"Error uploading data to th`e database: {e}")

# Testing class
if __name__ == "__main__":
    # Create an instance of DatabaseConnector with the local database connection
    database_connector_instance = DatabaseConnector().init_local_db_connector()

    # Test code to see if credentials load
    credentials = database_connector_instance.read_local_db_creds()
    if credentials:
        print("Database Credentials:")
        print(credentials)

    # Test upload to db 
    database_connector_instance.upload_to_db(DataCleaning().clean_user_data(), table_name='dim_users')
