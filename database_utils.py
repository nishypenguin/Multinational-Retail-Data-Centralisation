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

    def upload_to_db(self, db_cleaner, engine, table_name):
        cleaned_data = db_cleaner.clean_user_data()
        cleaned_data.to_sql(table_name, engine, if_exists = 'replace')



db_connector = DataBaseConnector()





