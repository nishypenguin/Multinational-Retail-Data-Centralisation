import pandas as pd
class DataCleaning:

    def clean_user_data(self, db_extractor):
        unclean_df = db_extractor.read_rds_table()
        return cleaned_df
        ## clean rest of data later
        
db_cleaner = DataCleaning()

