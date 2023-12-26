import pandas as pd
from database_extraction import *

class DataCleaning:
    def __init__(self, data):
        user_data_df = data_extractor_instance.read_rds_table('legacy_users') # (user_table_name)
        #print(user_data_df)
        self.data = user_data_df

    # Methods to clean data
    def clean_null_values(self):
        # Drop rows with NULL values
        cleaned_data = self.data.dropna()
        return print(cleaned_data)
        #return self.clean_date_errors(cleaned_data)
""" 
    def clean_date_errors(self, join_data):
        # 'join_data' is the name of the date column
        try:
            # Convert the date column to datetime format from object
            cleaned_date_errors = self.data[join_data] = pd.to_datetime(self.data[join_data], errors='coerce')
            print(cleaned_date_errors)
        except Exception as e:
            print(f"Error cleaning date errors: {e}")
        finally:
            return None #self.data 


    def clean_data_types(self, column, target_type):
        # Cleaning the 'address' column from mistyping and errors
        if column == 'address' and target_type == object:
            try:
                # Remove '\n' from the 'address' column with empty space
                self.data[column] = self.data[column].astype(str).str.replace('\n', ' ')
            except Exception as e:
                print(f"Error cleaning data types: {e}")
            return self.data
        else:
            # General case for converting data types from object to str
            try:
                # Convert the column to the target data type
                self.data[column] = self.data[column].astype(target_type)
            except Exception as e:
                print(f"Error cleaning data types: {e}")
            return self.data

    def clean_user_data(self):
        # Execution of cleaning methods
        cleaned_data = self.clean_null_values()
        cleaned_data = self.clean_date_errors('join_data')
        cleaned_data = self.clean_data_types('address', object)
        return print(cleaned_data)
  """   
#test code method by method
data_extractor_instance = DataExtractor()    
data_cleaning_instance = DataCleaning(None) #(data_extractor_instance).clean_user_data()
data_cleaning_instance.clean_null_values()
#data_cleaning_instance.clean_date_errors()