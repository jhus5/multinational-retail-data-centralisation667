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
    data_cleaning_instance = DataCleaning() #df
    #clean data
    cleaned_data = data_cleaning_instance.clean_data(df)
    #print(cleaned_data)
    
    #insert dataframe into postgresql db
    database_connector_instance.upload_to_db(cleaned_data, table_name='dim_users')
    
    #pdf_data extract, clean and upload to database
    dfs = data_extractor_instance.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    dfs = data_cleaning_instance.clean_card_data(dfs)
    database_connector_instance.upload_to_db(dfs, table_name='dim_card_details')

    ##api
    #header dictionary
    header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    no_stores_ep = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

    storenumber = data_extractor_instance.list_number_of_stores(header_dict, no_stores_ep)
    api_df = data_extractor_instance.retrieve_stores_data(header_dict, storenumber)

    #clean data
    store_details = data_cleaning_instance.called_clean_store_data(api_df)
    database_connector_instance.upload_to_db(store_details, table_name='dim_store_details')
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