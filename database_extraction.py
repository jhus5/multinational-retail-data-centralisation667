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
        dfs = tabula.read_pdf(self.pdf_path, pages='1', stream = True) # pages='all'
        #take first table from dataframes
        dfs = dfs[0]
        #print(df)
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


        s3_df = s3_df.dropna(how='all')
        
        #print(s3_df.weight[2])
        #print(len(s3_df['weight']))
        
        ##convert ml
        # contains_ml = s3_df[s3_df['weight'].str.contains(('ml'), na = False)]
        # contains_ml['weight'] = contains_ml['weight'].replace('[^\d.]','', regex=True).astype(float)
        # contains_ml['weight'] = contains_ml['weight']*0.001

        # s3_df['weight'] = s3_df.loc[s3_df['weight'].str.contains(('ml'), na = False)]
        # contains_ml['weight'] = contains_ml['weight'].replace('[^\d.]','', regex=True).astype(float)
        # contains_ml['weight'] = contains_ml['weight']*0.001

        #convert kg
        # contains_kg = s3_df[s3_df['weight'].str.contains(('kg'), na = False)]
        # contains_kg['weight'] = contains_kg['weight'].replace('[^\d.]','', regex=True).astype(float)
        
        #convert grams
        # contains_grams = s3_df[s3_df['weight'].str.contains(('g'), na = False)]
        #contains_grams['weight'] = contains_grams['weight'].replace('[^\d.]','', regex=True).astype(float)
        #contains_grams['weight'] = contains_grams['weight']*0.001
        
        # contains_oz = s3_df[s3_df['weight'].str.contains(('oz'), na = False)]
        # contains_oz['weight'] = contains_oz['weight'].replace('[^\d.]','', regex=True).astype(float)
        # contains_oz['weight'] = contains_oz['weight']*28.35*0.001
        
        # drop rows containing 'x' in weight column
        # contains_x = s3_df[s3_df['weight'].str.contains('x') == False]

        # contains_p = s3_df[s3_df['weight'].str.contains(('p'), na = False)]
        
        #s3_df['weight'] = s3_df['weight'].str.contains(('ml'), na = False).strip("ml")
        #s3_df['weight'] = 
        #s3_df['weight'] = 
        
        
        s3_df['weight'] = s3_df['weight'].str.strip('kg')
        s3_df['weight'] = s3_df['weight'].str.strip('ml')
        s3_df['weight'] = s3_df['weight'].str.strip('g')
        s3_df['weight'] = s3_df['weight'].str.rstrip('.')
        s3_df['weight'] = s3_df['weight'].str.replace('[A-Z]', '', regex=True)
        s3_df['weight'] = s3_df['weight'].str.replace('[x]', '*', regex=True)
        s3_df['weight'] = s3_df['weight'].str.replace('[a-z]', '', regex=True)
        s3_df['weight'] = s3_df['weight'].str.replace(' ', '')
        s3_df['weight'] = s3_df['weight'].replace(to_replace='1',value="1000")
        # to_replace '*'
        #s3_df.loc[s3_df['weight'].str.contains('*', na =False)] =  s3_df['weight'].replace(to_replace='*',value="MULTIYPLY")

        s = s3_df['weight'].str.split(pat='*', n = 1, expand=True)
        s3_df['quantity_w'] = s[0]
        s3_df['weight_q'] = s[1]
        #replace none with 1
        s3_df['weight_q'] = s3_df['weight_q'].fillna(1).astype(float)
        s3_df['weight_q'] = s3_df['weight_q'].astype(float)
        #convert to float
        s3_df['quantity_w'] = s3_df['quantity_w'].astype(float)

        #multiply and replace
        s3_df['weight_1'] = s3_df['quantity_w']*s3_df['weight_q']
        #replace weight with weight 1
        s3_df['weight'] = s3_df['weight_1']
        #drop columns
        s3_df = s3_df.drop(['quantity_w'], axis = 1)
        s3_df = s3_df.drop(['weight_q'], axis = 1)
        s3_df = s3_df.drop(['weight_1'], axis = 1)
        


        #print(mask)
        # print(contains_ml['weight'])
        # print(contains_grams['weight'])
        # print(contains_kg['weight'])
        # print(contains_oz['weight'])
        # print(contains_x['weight'])
        # print(s3_df['weight'])
        # print(s3_df['weight'][1748])
        print(s3_df)
        return s3_df



'''
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
'''