# Import required libraries
import yaml
import sqlalchemy
from sqlalchemy import create_engine, MetaData
import pandas as pd
#from database_connector import DatabaseConnector

class DataExtractor:
    def __init__(self):
        self.db_engine = self.init_db_engine()
        #self.db_connector = self.init_db_connector()

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
            #print(df.columns)
            return df
        else:
            print("Error: Database engine not initialized.")
            return None
    
# Create an instance of DataExtractor
data_extractor_instance = DataExtractor()

# Test code to see if credentials load
credentials = data_extractor_instance.read_db_creds()
if credentials:
    print("Database Credentials:")
    print(credentials) 

# Initialize the database engine
db_engine = data_extractor_instance.init_db_engine()

# Test read tables
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

#test df of tables
# List all tables in the database
#tables = data_extractor_instance.list_db_tables()
    
if db_engine:
    if tables:
        #print("Tables in the database:")
        #print(tables)

        # Assume 'users' is the table containing user data
        user_table_name = 'legacy_users'

        # Read data from the 'users' table into a DataFrame
        user_data_df = data_extractor_instance.read_rds_table(user_table_name)
        if user_data_df is not None:
            print(f"Data from '{user_table_name}' table:")
            #print(user_data_df)
            #print(list(user_data_df.columns))
            #print(user_data_df.iloc[[502]])
            print(user_data_df.head(10))
            print(user_data_df.dtypes)
            #print(user_data_df.info())
        else:
            print("Error Initializing Database Engine.")
            
