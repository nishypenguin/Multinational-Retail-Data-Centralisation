import yaml
from sqlalchemy import create_engine, inspect
import urllib.parse


class DatabaseConnector:

    def __init__(self, creds_file='db_creds.yaml'):
        """
        Initializes the connector by reading database credentials from the YAML file.
        """
        self.creds_file = creds_file
        self.creds = self.read_db_creds()
        self.engine = self.init_db_engine()

    def read_db_creds(self):
        try:
            with open(self.creds_file, 'r') as file:
                creds = yaml.safe_load(file)
                #print("Credentials successfully read:")
                #print(creds)  # Debug statement
                return creds
        except Exception as e:
            print(f"Error reading credentials file: {e}")
            return None
        
    def read_db_local_creds(self, local_creds_file):
        """
        Reads the database credentials from the specified YAML file.
        
        Args:
            local_creds_file (str): The path to the YAML file with database credentials.
        
        Returns:
            dict: The database credentials, or None if an error occurs.
        """
        try:
            with open(local_creds_file, 'r') as file:
                creds = yaml.safe_load(file)
                return creds
        except Exception as e:
            print(f"Error reading credentials file: {e}")
            return None

        

    def init_db_engine(self):
        """Initializes and returns an SQLAlchemy engine using the database credentials."""

    
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = self.creds['RDS_HOST']
        USER = urllib.parse.quote_plus(self.creds['RDS_USER'])
        PASSWORD = urllib.parse.quote_plus(self.creds['RDS_PASSWORD'])
        DATABASE = self.creds['RDS_DATABASE']
        PORT = self.creds['RDS_PORT']


        engine = create_engine(
            f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
        )
        return engine

    def list_db_tables(self):
        """Lists all tables in the connected database."""
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        return tables

    def upload_to_db(self, df, table_name, local_creds_file='db_creds_local.yaml'):
        """
        Uploads a DataFrame to the database by reading credentials from 'db_creds_local.yaml'.

        Args:
            df (pd.DataFrame): The DataFrame to upload.
            table_name (str): The name of the target table in the database.
            creds_file (str): The path to the credentials YAML file (defaults to 'db_creds_local.yaml').
        """
        # Read credentials from the specified file (defaults to db_creds_local.yaml)
        local_creds = self.read_db_local_creds(local_creds_file)

        # Initialize a new engine using the local credentials
        try:
            DATABASE_TYPE = 'postgresql'
            DBAPI = 'psycopg2'
            HOST = local_creds['RDS_HOST']
            USER = urllib.parse.quote_plus(local_creds['RDS_USER'])
            PASSWORD = urllib.parse.quote_plus(local_creds['RDS_PASSWORD'])
            DATABASE = local_creds['RDS_DATABASE']
            PORT = local_creds['RDS_PORT']

            local_engine = create_engine(
                f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
            )

            # Upload DataFrame to the database using the new engine
            df.to_sql(table_name, local_engine, if_exists='replace', index=False)
            print(f"Data uploaded successfully to table '{table_name}' using credentials from '{local_creds_file}'.")

        except Exception as e:
            print(f"Error uploading data to table '{table_name}': {e}")




