import pandas as pd
from datetime import datetime
from database_extraction import *

class DataCleaning:
    #def __init__(self, df) -> None:
    #    pass
     
    def __init__(self, df): 
        self.data = df
        #print(df)

    # Methods to clean data
    def clean_data(self):
        # Drop rows with NULL values
        df = self.data.dropna()
        

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
        dfs = dfs.dropna()
        

        ##standardise dates
        #remove non datetime
        dfs['date_payment_confirmed'] = pd.to_datetime(dfs['date_payment_confirmed'], format = 'mixed', errors='coerce')
        dfs = dfs.dropna(subset=['date_payment_confirmed'])
        #keep date only
        dfs['date_payment_confirmed'] = dfs['date_payment_confirmed'].dt.date
      
        print(dfs)
        return dfs
    
"""

    def clean_user_data(self):
        # Execution of cleaning methods
        cleaned_data = self.clean_data()
        #cleaned_data = self.clean_date_errors('join_date')
        #cleaned_data = self.clean_data_types('address', object)
        return print(cleaned_data)
"""   
#test code method by method
#data_extractor_instance = DataExtractor()    
#data_cleaning_instance = DataCleaning(None) #(data_extractor_instance).clean_user_data()
#data_cleaning_instance.clean_data()
#data_cleaning_instance.clean_date_errors()