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




'''
import pandas as pd
from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self, db_url):
        self.db_engine = create_engine(db_url)

    def upload_to_db(self, cleaned_data, table_name='dim_users'):
        try:
            # Convert DataFrame to SQL and upload to the specified table
            cleaned_data.to_sql(name=table_name, con=self.db_engine, if_exists='replace', index=False)
            print(f"Data uploaded to the '{table_name}' table successfully.")
        except Exception as e:
            print(f"Error uploading data to the database: {e}")
'''

'''
    #connect database    
    def init_db_connector(self):
        credentials = self.read_db_creds()
        if credentials:
            # Construct the database URL
            db_url = f"postgresql://{credentials.get('RDS_USER', '')}:{credentials.get('RDS_PASSWORD', '')}@{credentials.get('RDS_HOST', '')}:{credentials.get('RDS_PORT', '')}/{credentials.get('RDS_DATABASE', '')}"
            
            # Initialize and return the DatabaseConnector
            return DatabaseConnector(db_url)
        else:
            return None

    def upload_to_db(self, cleaned_data, table_name='dim_users'):
        if self.db_connector:
            self.db_connector.upload_to_db(cleaned_data, table_name)
        else:
            print("Error: Database connector not initialized.")
'''

#    def upload_to_db(self, cleaned_data, table_name='dim_users'):
#        # Upload cleaned data to the specified table in the local PostgreSQL database
#        cleaned_data.to_sql(table_name, con=self.db_engine, if_exists='replace', index=False)