import pandas as pd

class DataExtractor:

    def read_rds_table(self, db_connector, table_name):
        creds = db_connector.read_db_creds()
        engine = db_connector.init_db_engine(creds)
        table_names = db_connector.list_db_tables(engine)
        if table_name in table_names:
            unclean_df = pd.read_sql_table(table_name, engine)
            return unclean_df
           
db_extractor = DataExtractor()

        