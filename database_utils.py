#import psycopg2
import yaml 

class DataBaseConnector:

    def __init__(self, creds_file='db_creds.yaml'):
        self.creds_file = creds_file

    def read_db_creds(self):
       with open (self.creds_file, 'r') as file:
           creds = yaml.safe_load(file)
           return creds 


