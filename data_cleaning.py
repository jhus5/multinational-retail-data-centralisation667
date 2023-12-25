import pandas as pd

class DataCleaning:
    def __init__(self, data):
        self.data = data

    # Methods to clean data
    def clean_null_values(self):
        # Drop rows with NULL values
        cleaned_data = self.data.dropna()
        return cleaned_data

    def clean_date_errors(self, join_data):
        # 'join_data' is the name of the date column
        try:
            # Convert the date column to datetime format from object
            self.data[join_data] = pd.to_datetime(self.data[join_data], errors='coerce')
        except Exception as e:
            print(f"Error cleaning date errors: {e}")
        return self.data

    def clean_data_types(self, column, target_type):
        # Cleaning the 'address' column from mistyping and errors
        if column == 'address' and target_type == str:
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
        # Example usage of cleaning methods
        cleaned_data = self.clean_null_values()
        cleaned_data = self.clean_date_errors('join_data')
        cleaned_data = self.clean_data_types('age', int)
        return cleaned_data
