# Import required libraries
import yaml
#for db
import sqlalchemy
from sqlalchemy import create_engine, MetaData
import pandas as pd
#for pdf
import tabula
#for api
import requests
import json
#s3 buckets
import boto3
##regex
import re
#numpy
import numpy as np


class DataExtractor:
    def __init__(self, engine):
        self.db_engine = engine

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
        # Read remote pdf into list of DataFrame
        dfs = pd.concat(tabula.read_pdf(self.pdf_path, pages='all', multiple_tables=False)) # pages='1'
        
        #print(f"Found {len(dfs)} tables")
        #dfs = dfs[0]
        
        
        return dfs

    ##api extract and clean details of each store
    def list_number_of_stores(self, header_dict, no_stores_ep):
        response = requests.get(no_stores_ep, headers=header_dict)
        if response:
            print('Request is successful.')
            print(response.status_code)
            #print(response.text)
            print(response.json())
            my_json = response.json()
            no_of_stores = my_json['number_stores']
            return no_of_stores
        else:
            print('Request returned an error.')
            print(response.status_code)
            
    def retrieve_stores_data(self, header_dict, storenumber):
        #storenumber = self.list_number_of_stores(header_dict, no_stores_ep)
        data = []
        for store_number in range (0, storenumber):
            retrieve_a_store_ep = f'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'
            response = requests.get(retrieve_a_store_ep, headers=header_dict)
            #print(response.json())
            data.append(response.json())
        #cobert json to dataframe
        df = pd.json_normalize(data)
        # setting index as index column
        df.set_index("index", inplace = True)
        #print(df.head())
        return(df)

    #CSV from AWS (task 6)
    def extract_from_s3(self, url='s3://data-handling-public/products.csv'):
        self.path = url
        # s3 boto client and csv path
        s3_client = boto3.client('s3')
        #path = 's3://data-handling-public/products.csv'
        obj = s3_client.get_object(Bucket='data-handling-public', Key='products.csv')
        s3_df = pd.read_csv(obj['Body'], index_col=0)

        return s3_df

    #JSON from AWS (task 8)
    def extract_from_s3_json(self, url='https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'):
        self.json_path = url
        sales_df = pd.read_json(self.json_path)

        # Drop rows with NULL values
        sales_df = sales_df.dropna(how='all')

        #remove non-numeric charecters
        #sales_df['day'] = sales_df['day'].replace(r'[^0-9]+', '', regex=True)

        #remove non-numeric data
        sales_df['month'] = pd.to_numeric(sales_df['month'], errors="coerce")
        sales_df['year'] = pd.to_numeric(sales_df['year'], errors="coerce")
        sales_df['day'] = pd.to_numeric(sales_df['day'], errors="coerce")

        sales_df['timestamp'] = pd.to_datetime(sales_df['timestamp'], errors="coerce")
        sales_df['timestamp'] = sales_df['timestamp'].dt.time

        sales_df = sales_df[~sales_df.time_period.str.contains(r'[0-9]')]


        sales_df['time_period'] = sales_df['time_period'].replace('NULL',np.nan, regex=True)
        sales_df['date_uuid'] = sales_df['date_uuid'].replace('NULL',np.nan, regex=True)

        # Drop rows with NULL values
        sales_df = sales_df.dropna(how='all')

        return sales_df