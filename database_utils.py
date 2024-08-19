import psycopg2
import yaml 
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

class DataBaseConnector:

    def __init__(self, creds_file='db_creds.yaml'):
        self.creds_file = creds_file

    def read_db_creds(self):
       with open (self.creds_file, 'r') as file:
           creds = yaml.safe_load(file)
           return creds 

    def init_db_engine(self, creds):
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = creds['RDS_HOST']
        USER = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD']
        DATABASE = creds['RDS_DATABASE']
        PORT = creds['RDS_PORT']

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")
        engine.connect()
        return engine
    
    def list_db_tables(self, engine):
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        return table_names

db_connector = DataBaseConnector()

# Read the database credentials from the YAML file
creds = db_connector.read_db_creds()

# Initialize the database engine using the credentials
engine = db_connector.init_db_engine(creds)

# List all tables in the database
table_names = db_connector.list_db_tables(engine)

# Print the list of tables
print("Tables in the database:", table_names)
    




