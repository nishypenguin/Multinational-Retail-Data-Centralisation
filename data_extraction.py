import pandas as pd
import tabula
import requests
from urllib.parse import urlparse
import boto3
from io import StringIO
from database_utils import DatabaseConnector
import json

class DataExtractor:

    def read_rds_table(self, db_connector, table_name):
        """
        Extracts a table from the RDS database into a pandas DataFrame.

        Args:
            db_connector (DatabaseConnector): An instance of DatabaseConnector.
            table_name (str): The name of the table to extract.

        Returns:
            pd.DataFrame: The extracted data.
        """
        # Use the engine from db_connector
        engine = db_connector.engine

        # Check if table exists
        table_names = db_connector.list_db_tables()
        if table_name in table_names:
            unclean_df = pd.read_sql_table(table_name, engine)
            print(f"Extracted data from table: {table_name}")
            return unclean_df
        else:
            raise ValueError(f"Table '{table_name}' does not exist in the database.")

    def retrieve_pdf_data(self, pdf_link):
        """
        Extracts tables from a PDF file at the given link and returns a pandas DataFrame.

        Args:
            pdf_link (str): The URL to the PDF file.

        Returns:
            pd.DataFrame: The extracted data as a DataFrame.
        """
        try:
            # Read all tables from the PDF
            dfs = tabula.read_pdf(pdf_link, pages='all', multiple_tables=True)
            # Concatenate all DataFrames into one
            combined_df = pd.concat(dfs, ignore_index=True)
            print("Data extracted from PDF successfully.")
            return combined_df
        except Exception as e:
            print(f"Error extracting data from PDF: {e}")
            return None
        

    def list_number_of_stores(self, number_stores_endpoint, headers):
        """
        Retrieves the number of stores from the API.

        Args:
            number_stores_endpoint (str): The API endpoint to get the number of stores.
            headers (dict): The headers including the API key.

        Returns:
            int: The number of stores.
        """
        try:
            response = requests.get(number_stores_endpoint, headers=headers)
            response.raise_for_status()  # Raise an error for bad status codes
            data = response.json()
            number_of_stores = data.get('number_stores')
            print(f"Number of stores retrieved: {number_of_stores}")
            return number_of_stores
        except Exception as e:
            print(f"Error retrieving number of stores: {e}")
            return None
        
    
    def retrieve_stores_data(self, store_endpoint, headers, number_of_stores):
        """
        Retrieves data for all stores from the API and returns a pandas DataFrame.

        Args:
            store_endpoint (str): The API endpoint to retrieve a store's details.
            headers (dict): The headers including the API key.
            number_of_stores (int): The total number of stores to retrieve.

        Returns:
            pd.DataFrame: DataFrame containing all store details.
        """
        store_data_list = []
        for store_number in range(number_of_stores):
            try:
                url = store_endpoint.format(store_number=store_number)
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                data = response.json()
                store_data_list.append(data)
            except Exception as e:
                print(f"Error retrieving data for store {store_number}: {e}")
                continue  # Skip to the next store if there's an error

        # Convert list of dictionaries to DataFrame
        stores_df = pd.DataFrame(store_data_list)
        print("All store data retrieved successfully.")
        #print(stores_df.info())
        return stores_df
    
    def extract_from_s3(self, s3_address):
        """
        Downloads a CSV file from the specified S3 address and returns it as a pandas DataFrame.

        Args:
            s3_address (str): The S3 address of the file (e.g., 's3://bucket-name/file.csv').

        Returns:
            pd.DataFrame: The data from the CSV file as a pandas DataFrame.
        """
        # Parse the S3 address
        parsed_url = urlparse(s3_address)
        bucket_name = parsed_url.netloc
        key = parsed_url.path.lstrip('/')

        # Initialize the S3 client using boto3
        s3 = boto3.client('s3')

        try:
            # Download the file from S3 into memory
            obj = s3.get_object(Bucket=bucket_name, Key=key)
            csv_data = obj['Body'].read().decode('utf-8')

            # Read the CSV data into a pandas DataFrame
            df = pd.read_csv(StringIO(csv_data))

            print(f"Data extracted from {s3_address} successfully.")
            return df
        except Exception as e:
            print(f"Error extracting data from S3: {e}")
            return None
        
    def extract_from_s3_json(self, s3_address):
        """
        Downloads a JSON file from the specified S3 address and returns it as a pandas DataFrame.

        Args:
            s3_address (str): The S3 address of the file (e.g., 's3://bucket-name/file.json').

        Returns:
            pd.DataFrame: The data from the JSON file as a pandas DataFrame.
        """

        # Initialize the S3 client using boto3
        s3 = boto3.client('s3')

        try:
            # Download the JSON data from the URL
            response = requests.get(s3_address)

            # Check if the request was successful
            if response.status_code == 200:
                json_data = response.json()
                
                # Convert the JSON data into a pandas DataFrame
                df = pd.DataFrame(json_data)

            print(f"Data extracted from {s3_address} successfully.")
            return df
        except Exception as e:
            print(f"Error extracting data from S3: {e}")
            return None

