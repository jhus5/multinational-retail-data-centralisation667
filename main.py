## import other files
from database_extraction import *
from database_connector import *
from data_cleaning import *



# Testing class
if __name__ == "__main__":
    
    # Create an instance of DatabaseConnector with the local database connection
    database_connector_instance = DatabaseConnector()

    #initialise db engine
    # Initialize the database engine
    db_engine = database_connector_instance.init_local_db_connector()

    # Test code to read tables
    if db_engine:
        print("Database Engine Initialized Successfully.")

    # Create an instance of DataExtractor
    data_extractor_instance = DataExtractor()

    # List all tables in the database
    tables = data_extractor_instance.list_db_tables()
    if tables:
        print("Tables in the database:")
        print(tables)
    
    #table to dataframe
    df = data_extractor_instance.read_rds_table('legacy_users')
    #print(df)

    #data cleaning instance
    data_cleaning_instance = DataCleaning(df)
    #clean data
    cleaned_data = data_cleaning_instance.clean_data()
    print(cleaned_data)
    
    #insert dataframe into postgresql db
    database_connector_instance.upload_to_db(cleaned_data, table_name='dim_users')
    

'''
def main ():
    #data extractor instance
    data_extractor_instance = DataExtractor()
    db_engine = data_extractor_instance.init_db_engine()    

    if db_engine:
        tables = data_extractor_instance.list_db_tables()
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

    #data cleaning instance
    #data_cleaning_instance = DataCleaning() #(data_extractor_instance).clean_user_data()
    #data_extractor_instance = DataExtractor()
    
    #user_data_df = data_extractor_instance.read_rds_table('legacy_users') # (user_table_name)
    #print(user_data_df)
    #data_cleaning_instance(user_data_df)


if __name__ == '__main__':
	main()
'''