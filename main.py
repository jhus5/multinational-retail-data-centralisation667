## import other files
from database_extraction import *
from database_connector import *
from data_cleaning import *



# Testing class
if __name__ == "__main__":
    
    yaml_file = 'db_creds.yaml' 
    # Create an instance of DatabaseConnector with the local database connection
    database_connector_instance = DatabaseConnector(yaml_file)
    engine = database_connector_instance.engine

    # Create an instance of DataExtractor
    data_extractor_instance = DataExtractor(engine)

    # ######################
    another_yaml_file = 'local_db_creds.yaml' 
    # Create an instance of DatabaseConnector with the local database connection
    another_database_connector_instance = DatabaseConnector(another_yaml_file)
    another_engine = database_connector_instance.engine

    # ################

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
    another_database_connector_instance.upload_to_db(cleaned_data, table_name='dim_users')
    
    #pdf_data extract, clean and upload to database
    dfs = data_extractor_instance.retrieve_pdf_data('https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf')
    #print(dfs)
    dfs = data_cleaning_instance.clean_card_data(dfs)
    #print(dfs)
    another_database_connector_instance.upload_to_db(dfs, table_name='dim_card_details')

    
    ##api
    #header dictionary
    header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    no_stores_ep = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'

    storenumber = data_extractor_instance.list_number_of_stores(header_dict, no_stores_ep)
    api_df = data_extractor_instance.retrieve_stores_data(header_dict, storenumber)

    #clean data
    store_details = data_cleaning_instance.called_clean_store_data(api_df)
    another_database_connector_instance.upload_to_db(store_details, table_name='dim_store_details')

    #aws csv
    csv_df = data_extractor_instance.extract_from_s3()

    #clean aws csv
    s3_df = data_cleaning_instance.convert_product_weights(csv_df)
    product_details = data_cleaning_instance.clean_products_data(s3_df)
    another_database_connector_instance.upload_to_db(product_details, table_name='dim_products')

    #Task 7 retrieve and clean orders table
    # List all tables in the database
    tables = data_extractor_instance.list_db_tables()
    if tables:
        print("Tables in the database:")
        print(tables)
    
    #orders table to dataframe
    orders_df = data_extractor_instance.read_rds_table('orders_table')
    #print(orders_df)
    
    #clean orders table
    cleaned_orders_df = data_cleaning_instance.clean_orders_data(orders_df)
    #print(cleaned_orders_df)
    another_database_connector_instance.upload_to_db(cleaned_orders_df, table_name='orders_table')

    #Milestone 2 Task 8
    sales_df = data_extractor_instance.extract_from_s3_json()
    #print(sales_df)
    #any cleaning
    ##move to data
    another_database_connector_instance.upload_to_db(sales_df, table_name='dim_date_times') #upload sales data


    