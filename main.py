import pandas as pd
from data_extraction import DataExtractor
from database_utils import DatabaseConnector
from data_cleaning import DataCleaning

def main():
    try:
        # Step 1: def extract_from_s3_json(self, s3_address)
        db_extractor = DataExtractor()
        s3_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
        unclean_df = db_extractor.extract_from_s3_json(s3_address)
        
        # Step 2: Clean data
        db_cleaner = DataCleaning()
        clean_df=  db_cleaner.clean_sales_details(unclean_df)
        
        # Step 3: Upload the cleaned data to the database, overwriting 'orders_table'.
        db_connector = DatabaseConnector()
        db_connector.upload_to_db(clean_df, 'dim_date_times', local_creds_file='db_creds_local.yaml')
        print("Data upload complete.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

