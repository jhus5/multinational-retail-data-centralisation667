import yaml
import pandas as pd
from sqlalchemy import create_engine
#from sqlalchemy_utils import database_exists, create_database
from data_cleaning import *

class DatabaseConnector:

    def __init__(self, yaml_file):
        self.yaml_file = yaml_file
        self.credentials = self.read_db_creds(yaml_file)
        self.engine = self.db_connector()

    
    def read_db_creds(self, file_path):
        try:
            with open(file_path, 'r') as file:
                local_credentials = yaml.safe_load(file)
            return local_credentials
        except FileNotFoundError:
            print(f"Error: The file {file_path} was not found.")
        except yaml.YAMLError as e:
            print(f"Error reading YAML file: {e}")
        return None

    def db_connector(self):
        local_credentials = self.read_db_creds(self.yaml_file)
        if local_credentials:
            # Change the database URL to connect to a local PostgreSQL database
   
            db_url = f"postgresql+psycopg2://{local_credentials['RDS_USER']}:{local_credentials['RDS_PASSWORD']}@{local_credentials['RDS_HOST']}:{local_credentials['RDS_PORT']}/{local_credentials['RDS_DATABASE']}"

            print(db_url)
            # Initialize and return the DatabaseConnector with db_url
            engine = create_engine(db_url)
            return engine
        else:
            return None

    def upload_to_db(self, cleaned_data, table_name): 
        try:
            # Convert DataFrame to SQL and upload to the specified table
            cleaned_data.to_sql(name=table_name, con=self.engine, if_exists='replace', index=False)
            print(f"Data uploaded to the '{table_name}' table successfully.")
        except Exception as e:
            print(f"Error uploading data to th'e database: {e}")

