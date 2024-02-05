import pandas as pd
import numpy as np
from datetime import datetime
from database_extraction import *

class DataCleaning:
    #def __init__(self, df) -> None:
    #    pass
     
    def __init__(self): 
        None
        #self.data = df
        #print(df)

    # Methods to clean data
    def clean_data(self, df):
        # Drop rows with NULL values
        df = df.dropna(how='all')
        

        ##standardise dates
        #remove non datetime
        df['join_date'] = pd.to_datetime(df['join_date'], format = 'mixed', errors='coerce')
        df = df.dropna(subset=['join_date'])
        #keep date only
        df['join_date'] = df['join_date'].dt.date
      
        ##fix address by stripping new line ('\n') 
        # Removing new line expression
        df = df.replace('\n',' ', regex=True)
        
        return df
    
    #pdf table data task #4 
    def clean_card_data(self, dfs):
        
        # Drop rows with NULL values
        dfs = dfs.dropna(how='all')

        # strip leading charecter '?' from card_number
        dfs['card_number'] = dfs['card_number'].apply(lambda x: x.lstrip("?") if isinstance(x, str) else x)
        
        ##standardise dates
        #remove non datetime
        dfs['date_payment_confirmed'] = pd.to_datetime(dfs['date_payment_confirmed'], format = 'mixed', errors='coerce')
        dfs = dfs.dropna(subset=['date_payment_confirmed'])
        #keep date only
        dfs['date_payment_confirmed'] = dfs['date_payment_confirmed'].dt.date
      
        #print(dfs)
        return dfs
    
    #clean data from api - task 5
    def called_clean_store_data(self, data):
        self.csdata = data

        # Drop rows with NULL values
        self.csdata = self.csdata.dropna(how='all')

        ##standardise dates
        #convert to correct format
        self.csdata['opening_date'] = pd.to_datetime(self.csdata['opening_date'], format = 'mixed', errors='coerce')
        
        #remove empty rows
        self.csdata = self.csdata.dropna(subset=['opening_date'])

        #replace n/a
        self.csdata = self.csdata.replace('N/A',np.nan)

        #remove non-numeric charecters
        self.csdata['staff_numbers'] = self.csdata['staff_numbers'].replace(r'[^0-9]+', '', regex=True)
        
        #keep date only
        #self.csdata['opening_date'] = self.csdata['opening_date'].dt.date
      
        ##fix address by stripping new line ('\n') 
        # Removing new line expression
        self.csdata['address'] = self.csdata['address'].replace('\n',' ', regex=True)
        
        csdata = self.csdata

        #print(self.csdata.head())
        return csdata
    

    #######################
    #######################
    def convert_product_weights(self, s3_df):
        ##the following code should go to data_cleaning class 
        s3_df = s3_df.dropna(how='all')

        #remove whitespace to all strings in column
        s3_df['weight'] = s3_df['weight'].apply(lambda x: x.strip() if isinstance(x, str) else x)
        s3_df['weight'] = s3_df['weight'].apply(lambda x: x.rstrip(".") if isinstance(x, str) else x)
        s3_df['weight'] = s3_df['weight'].apply(lambda x: x.rstrip(" ") if isinstance(x, str) else x)
        #for item in s3_df['weight']:
        #      print(item)

        def convert_to_kg(x):
            if 'x' in x:
                # Extract numerical values for quantity and weight
                quantity, weight_value = re.findall(r'\d+', x)
                weight_in_kg = int(quantity) * float(weight_value) / 1000
                return weight_in_kg
            
            elif 'kg' in x:
                return float(x[:-2]) * 1
            elif 'ml' in x:
                return float(x[:-2]) * 0.001
            elif 'oz' in x:
                return float(x[:-2]) * 28.35 * 0.001
            elif 'g' in x:
                return float(x[:-1]) * 0.001
            else:
                pass

        s3_df['weight'] = s3_df['weight'].apply(convert_to_kg)

        #print(s3_df)
        return s3_df

                
    ################################
    def clean_products_data(self,s3_df):
        # Filter 'product_price' column to keep only those that start with '£'
        s3_df = s3_df[s3_df.product_price.str.startswith(('£'))]
        #print(s3_df)
        return s3_df


    #######################
    #######################
    def clean_orders_data(self, orders_df):
        orders_df = orders_df.drop(columns = ['first_name', 'last_name', '1'])
        # # Drop rows with NULL values
        # orders_df = orders_df.dropna(how='all')
        # #remove non-numeric charecters
        # orders_df['day'] = orders_df['day'].replace(r'[^0-9]+', '', regex=True)
        return orders_df

