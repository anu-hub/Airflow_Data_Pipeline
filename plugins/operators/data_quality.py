from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
 

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        songplay_check = (""" SELECT count(1) from songplays where start_time IS NULL;""")
        users_check = (""" SELECT count(1) from users where userid IS NULL;""")
        songs_check = (""" SELECT count(1) from songs where songid IS NULL or artistid IS NULL;""")
        artists_check = (""" SELECT count(1) from artists where artistid IS NULL;""")
        time_check = (""" SELECT count(1) from time where start_time IS NULL;""")
        data_quality_checks = [songplay_check, users_check, songs_check, artists_check, time_check]
        
        self.log.info('DataQualityOperator starting')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        
        for check in data_quality_checks:
            records = redshift_hook.get_records(check)[0]
            logging.info(f"Record Count. {records[0]}")
            if records[0] != 0:
                logging.info(f"Record Count. {records[0]}")    
                raise ValueError(f"Data quality check failed.{check}")
            else:
                logging.info(f"Data quality check completed. {check}")
                logging.info(f"Record Count. {records[0]}")