# Import required libraries
import yaml
import sqlalchemy
from sqlalchemy import create_engine, MetaData
import pandas as pd

import tabula

class DataExtractor:
    def __init__(self):
        self.db_engine = self.init_db_engine()

    def read_db_creds(self, file_path='db_creds.yaml'):
        try:
            with open(file_path, 'r') as file:
                credentials = yaml.safe_load(file)
            return credentials
        except FileNotFoundError:
            print(f"Error: The file {file_path} was not found.")
        except yaml.YAMLError as e:
            print(f"Error reading YAML file: {e}")

    def init_db_engine(self):
        credentials = self.read_db_creds()
        if credentials:
            # Construct the database URL
            db_url = f"postgresql://{credentials.get('RDS_USER', '')}:{credentials.get('RDS_PASSWORD', '')}@{credentials.get('RDS_HOST', '')}:{credentials.get('RDS_PORT', '')}/{credentials.get('RDS_DATABASE', '')}"

            # Initialize and return the SQLAlchemy engine
            engine = create_engine(db_url)
            return engine
        else:
            return None

    def list_db_tables(self):
        if self.db_engine:
            # Create a MetaData object
            metadata = MetaData()

            # Reflect all tables in the database
            metadata.reflect(bind=self.db_engine)

            # Get table names
            table_names = metadata.tables.keys()
            return table_names
        else:
            print("Error: Database engine not initialized.")
            return None
        
    def read_rds_table(self, table_name):
        if self.db_engine:
            # Use SQLAlchemy to query and fetch data
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql_query(query, self.db_engine, index_col='index')
            #print(df)
            return df
        else:
            print("Error: Database engine not initialized.")
            return None
    
    #retrive pdf table data task #4
    def retrieve_pdf_data(self, pdf_path):
        #pdf_path = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
        self.pdf_path = pdf_path
        pass
        # Read remote pdf into list of DataFrame
        dfs = tabula.read_pdf(self.pdf_path, pages='1', stream = True) # pages='all'
        #take first table from dataframes
        dfs = dfs[0]
        #print(df)
        return dfs

"""     
# Create an instance of DataExtractor
data_extractor_instance = DataExtractor()


# Test code to see if credentials load
credentials = data_extractor_instance.read_db_creds()
if credentials:
    print("Database Credentials:")
    print(credentials) 

# Initialize the database engine
db_engine = data_extractor_instance.init_db_engine()

# Test code to read tables
if db_engine:
    print("Database Engine Initialized Successfully.")
    # You can now use 'db_engine' to interact with the database

    # List all tables in the database
    tables = data_extractor_instance.list_db_tables()
    if tables:
        print("Tables in the database:")
        print(tables)
else:
    print("Error Initializing Database Engine.")

#Test code to observe data from AWS database
## List all tables in the database
##tables = data_extractor_instance.list_db_tables() - this is already
    
if db_engine:
    if tables:
        #print("Tables in the database:")
        #print(tables)

        # 'legacy_users' is the table containing user data
        user_table_name = 'legacy_users'

        # Read data from the 'legacy_users' table into a DataFrame
        user_data_df = data_extractor_instance.read_rds_table(user_table_name)
        if user_data_df is not None:
            print("display from dataextractor class")
            #print(f"Data from '{user_table_name}' table:")
            #print(user_data_df)
            #print(list(user_data_df.columns))
            #print(user_data_df.iloc[[502]])
            #print(user_data_df.head(10))
            #print(user_data_df.dtypes)
            #print(user_data_df.info())
            #print("Printed from DataExtractor class")
        else:
            print("Error Initializing Database Engine.")
  """