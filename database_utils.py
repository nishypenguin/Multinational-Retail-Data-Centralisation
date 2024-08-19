import psycopg2
import yaml 

class DataBaseConnector:

    def __init__(self, creds_file='db_creds.yaml'):
        self.creds_file = creds_file

    def read_db_creds(self):
       with open (self.creds_file, 'r') as file:
           creds = yaml.safe_load(file)
           return creds 

    def init_db_engine(self,creds):
        HOST = creds['RDS_HPST']
        USER = creds['RDS_USER']
        PASSWORD = creds['RDS_PASSWORD']
        DATABASE = creds['RDS_DATABASE']
        PORT = creds['RDS_PORT']

        with psycopg2.connect(host=HOST, user=USER, password=PASSWORD, dbname=DATABASE, port=PORT) as conn:
            with conn.cursor() as cur:
                cur.execute('''CREATE TABLE actor_2 AS (
                            SELECT * FROM actor
                            LIMIT 10);

                            SELECT * FROM actor_2''')
                print(type(cur))
                records = cur.fetchall()





