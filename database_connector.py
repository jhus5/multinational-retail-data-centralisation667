#import libraries
import yaml
import pandas as pd
import psycopg2
#from sqlalchemy import create_engine
from data_cleaning import DataCleaning

class DatabaseConnector:
    def __init__(self):
        self.db_connector = self.init_local_db_connector()
    
    # Reading credentials for local data (seperate file from cloud db)
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

    # Establish connection to local postgres database
    def init_local_db_connector(self):
        credentials = self.read_local_db_creds()
        conn = None
        cur = None
        try:
            conn = psycopg2.connect(user = credentials.get['RDS_USER', ''], password = credentials.get['RDS_PASSWORD', ''], host = credentials.get['RDS_HOST', ''], port = credentials.get['RDS_PORT', ''], dbname = credentials.get['RDS_DATABASE', ''])
            cur = conn.cursor() 
            return cur
        except Exception as error:
            print("Error: connection not initialised.")
        
        finally:
            if conn is not None:
                conn.close()
            if cur is not None:
                cur.close() 

    # Upload cleaned table data to local database
    def upload_to_db(self, cleaned_data, table_name='dim_users'):
        if self.db_connector:
            self.db_connector.upload_to_db(cleaned_data, table_name)
        else:
            print("Error: Uploading error to table.")

