import pandas as pd

class DataCleaning:

    def clean_user_data(self, df):
        """
        Cleans the user data DataFrame.

        Args:
            df (pd.DataFrame): The raw user data.

        Returns:
            pd.DataFrame: The cleaned user data.
        """
        # Remove rows with NULL values
        df = df.dropna()

        # Convert date columns to datetime
        date_columns = ['date_of_birth', 'join_date']  # Adjust column names as needed
        for col in date_columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # Trim leading and trailing whitespace from string columns
        str_columns = ['first_name', 'last_name', 'company', 'email_address', 
                       'address', 'country', 'country_code', 'phone_number', 'user_uuid']
        for col in str_columns:
            df[col] = df[col].str.strip()

        # Remove rows with invalid dates
        df = df.dropna(subset=date_columns)

        # Remove the 'index' column if it is redundant
        if 'index' in df.columns:
            df = df.drop(columns=['index'])

        # Remove duplicates
        df = df.drop_duplicates()

        # Additional cleaning steps as necessary
        df = df.reset_index(drop=True)

        return df

    def clean_card_data(self, df):
        """
        Cleans the card data DataFrame by removing erroneous values,
        NULL values, and formatting errors.

        Args:
            df (pd.DataFrame): The raw card data.

        Returns:
            pd.DataFrame: The cleaned card data.
        """
        # Drop rows with all NULL values
        df = df.dropna(how='all')

        # Remove any duplicate rows
        df = df.drop_duplicates()

        # Standardize column names
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        # Remove rows with NULL values in critical columns
        critical_columns = ['card_number', 'expiry_date', 'card_provider', 'date_payment_confirmed']
        df = df.dropna(subset=critical_columns)

        # Convert 'date_payment_confirmed' to datetime
        df['date_payment_confirmed'] = pd.to_datetime(df['date_payment_confirmed'], errors='coerce')

        # Remove rows where 'date_payment_confirmed' could not be converted
        df = df.dropna(subset=['date_payment_confirmed'])

        # Trim whitespace from string columns
        string_columns = ['card_number', 'expiry_date', 'card_provider']
        for col in string_columns:
            df[col] = df[col].astype(str).str.strip()

        # Reset index
        df = df.reset_index(drop=True)

        print("Card data cleaned successfully.")
        return df


    def clean_store_data(self, df):
        """
        Cleans the store data DataFrame.

        Args:
            df (pd.DataFrame): The raw store data.

        Returns:
            pd.DataFrame: The cleaned store data.
        """
        # Remove duplicates
        df = df.drop_duplicates()

        # Drop rows with missing essential values
        essential_columns = ['store_code', 'address', 'store_type', 'latitude', 'longitude', 'locality', 'country_code']
        df = df.dropna(subset=essential_columns)

        # Trim whitespace from string columns
        str_columns = ['store_code', 'address', 'store_type', 'locality', 'country_code']
        for col in str_columns:
            df[col] = df[col].astype(str).str.strip()

        # Convert 'staff_numbers' to numeric
        df['staff_numbers'] = pd.to_numeric(df['staff_numbers'], errors='coerce')

        # Convert 'opening_date' to datetime
        df['opening_date'] = pd.to_datetime(df['opening_date'], errors='coerce')

        # Remove rows with invalid dates
        df = df.dropna(subset=['opening_date'])

        # Ensure 'latitude' and 'longitude' are numeric
        df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
        df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
        df = df.dropna(subset=['latitude', 'longitude'])

        # Reset index
        df = df.reset_index(drop=True)

        print("Store data cleaned successfully.")
        return df
    
    def convert_product_weights(self, df):
        """
        Cleans and standardizes the 'weight' column in the products DataFrame.

        Args:
            df (pd.DataFrame): The raw products DataFrame with a 'weight' column.

        Returns:
            pd.DataFrame: The DataFrame with the 'weight' column converted to kilograms (float).
        """
        
        # Iterate over the weight column and clean/convert each value
        for i in range(len(df)):
            value = df.at[i, 'weight']
            try:
                # Ensure the value is a string before stripping it
                if isinstance(value, str):
                    value = value.strip()

                    # Check for 'kg' and remove it, then convert to float
                    if 'kg' in value.lower():
                        df.at[i, 'weight'] = float(value.lower().replace('kg', '').strip())
                    
                    # Check for 'g', convert to kg (1g = 0.001kg)
                    elif 'g' in value.lower():
                        df.at[i, 'weight'] = float(value.lower().replace('g', '').strip()) / 1000
                    
                    # Check for 'ml', assume 1ml ≈ 1g and convert to kg
                    elif 'ml' in value.lower():
                        df.at[i, 'weight'] = float(value.lower().replace('ml', '').strip()) / 1000
                    
                    # Check for 'oz', convert to kg (1oz ≈ 28.3495g)
                    elif 'oz' in value.lower():
                        df.at[i, 'weight'] = float(value.lower().replace('oz', '').strip()) * 28.3495 / 1000
                    
                    # If no unit is provided, assume the value is in kg
                    else:
                        df.at[i, 'weight'] = float(value)
                # If the value is already a float, keep it as is
                elif isinstance(value, (int, float)):
                    df.at[i, 'weight'] = float(value)

            except ValueError:
                # If conversion fails, set the value to NaN
                df.at[i, 'weight'] = pd.NA
        
        # Drop rows with invalid weights
        df = df.dropna(subset=['weight'])

        # Reset the index after dropping rows
        df = df.reset_index(drop=True)

        return df
    
    def clean_products_data(self, df):
        """
        Cleans the product data DataFrame by removing erroneous values,
        standardizing columns, converting data types, and handling missing data.

        Args:
            df (pd.DataFrame): The raw product data.

        Returns:
            pd.DataFrame: The cleaned product data.
        """
        # Drop the 'Unnamed: 0' column if it exists
        df = df.drop(columns=['Unnamed: 0'], errors='ignore')

        # Standardize column names (convert to lowercase and replace spaces with underscores)
        df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')

        # Convert 'product_price' to numeric, handle errors by coercing invalid values to NaN
        #df['product_price'] = pd.to_numeric(df['product_price'], errors='coerce')

        # Convert 'date_added' to datetime, handle errors by coercing invalid values to NaT
        df['date_added'] = pd.to_datetime(df['date_added'], errors='coerce')

        # Ensure 'weight' is already cleaned and converted to float (from the convert_product_weights method)
        df['weight'] = pd.to_numeric(df['weight'], errors='coerce')

        # Remove the currency symbol and convert the column to float
        df['product_price'] = df['product_price'].str.replace('£', '').astype(float)

        # Trim whitespace from all string columns
        str_columns = ['product_name', 'category', 'ean', 'uuid', 'removed', 'product_code']
        for col in str_columns:
            df[col] = df[col].str.strip()

        # Reset index after cleaning
        df = df.reset_index(drop=True)

        print("Products data cleaned and data types converted successfully.")
        return df
    

    def clean_orders_data(self, df):
        """
        Cleans the orders data by removing unnecessary columns and preparing it 
        for upload to the database. The table will act as the source of truth for sales data.

        Args:
            df (pd.DataFrame): The raw orders data DataFrame.

        Returns:
            pd.DataFrame: The cleaned orders data DataFrame.
        """
        # Remove unnecessary columns: 'first_name', 'last_name', and '1'
        columns_to_remove = ['first_name', 'last_name', '1']
        df = df.drop(columns=columns_to_remove, errors='ignore')

        # Remove any duplicate rows if needed
        df = df.drop_duplicates()

        # Reset the index to ensure it is clean
        df = df.reset_index(drop=True)

        print("Orders data cleaned successfully.")
        return df
    
    def clean_sales_details(self,df):
        
        """
        Cleans the date-related data in the DataFrame. Converts separate year, month, day, and timestamp
        columns into a single 'datetime' column.

        Args:
            df (pd.DataFrame): The DataFrame containing the raw data.

        Returns:
            pd.DataFrame: The cleaned DataFrame.
        """
        try:
            # Combine year, month, day, and timestamp into a single 'datetime' column
            df['datetime'] = pd.to_datetime(df['year'] + '-' + df['month'] + '-' + df['day'] + ' ' + df['timestamp'],
                                            errors='coerce', format='%Y-%m-%d %H:%M:%S')

            # Drop the original individual columns now that we have 'datetime'
            df = df.drop(columns=['year', 'month', 'day', 'timestamp'])

            # Optional: Ensure 'time_period' is consistent (if needed)
            df['time_period'] = df['time_period'].str.capitalize()  # Capitalize just to ensure consistency
            
            # Reset index (optional if you want a clean index after cleaning)
            df = df.drop_duplicates().reset_index(drop=True)

            # Drop rows where the 'datetime' column has null (NaT) values
            df = df.dropna(subset=['datetime'])

            # Reset the index after dropping rows (optional)
            df = df.reset_index(drop=True)


            print("Data cleaned successfully.")
            return df

        except Exception as e:
            print(f"Error during data cleaning: {e}")
            return None
