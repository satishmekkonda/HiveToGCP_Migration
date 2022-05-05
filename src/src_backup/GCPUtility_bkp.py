from google.cloud import bigquery
import pandas as pd
from pyhive import hive
import config.constants as constants
import AppRunner as App

class GCPUtil:
    
    table_names = []
    def __init__(self):
        self.table_names =[]
       

    def GenerateBQLoad(self):
        conf=constants.variables()
        try:
            if conf.db_table=='ALL':
                conn=hive.Connection(host='localhost', database=conf.source_db)
                query='SHOW TABLES'
                result=pd.read_sql(query,conn)
                self.table_names.extend(list(result.iloc[:,0]))
                print(self.table_names)
                for table in self.table_names:
                    client = bigquery.Client(project=conf.project_name)
                    table_ref = client.dataset(conf.destination_db).table(table)
                    job_config = bigquery.LoadJobConfig()
                    job_config.source_format = bigquery.SourceFormat.ORC
                    job_config.autodetect = False
                    job_config.write_disposition='WRITE_APPEND'
                    job = client.load_table_from_uri(['gs://bqload_python_source/distcp/employees_orc/000000_0','gs://bqload_python_source/distcp/employees_orc/000000_0_copy_1'], table_ref, job_config=job_config)

                    # job is async operation so we have to wait for it to finish
                    job.result()
                    return self.table_names.extend(list(result.iloc[:,0]))


            else:
                self.table_names.extend(conf.db_table)
                #print(self.table_names)
             
                # upload to BigQuery
                '''
                for table in table_names:
                    client = bigquery.Client(project=conf.project_name)
                    table_ref = client.dataset(conf.destination_db).table(table)
                    job_config = bigquery.LoadJobConfig()
                    job_config.source_format = bigquery.SourceFormat.ORC
                    job_config.autodetect = False
                    job_config.write_disposition='WRITE_APPEND'
                    job = client.load_table_from_uri(['gs://bqload_python_source/distcp/employees_orc/000000_0','gs://bqload_python_source/distcp/employees_orc/000000_0_copy_1'], table_ref, job_config=job_config)

                    # job is async operation so we have to wait for it to finish
                    job.result()
                 '''   

        except Exception as e:
            conf.logger.error(f'{type(e).__name__} Exception Occured')
            print(type(e).__name__)
            raise e            

    def ExecuteBQLoad(self):
         # upload to BigQuery
        conf=constants.variables()
        try:
            print(App.GCP.table_names)
            for table in App.GCP.table_names:
                client = bigquery.Client(project=project_name)
                table_ref = client.dataset(destination_db).table(table)
                job_config = bigquery.LoadJobConfig()
                job_config.source_format = bigquery.SourceFormat.ORC
                job_config.autodetect = False
                job_config.write_disposition='WRITE_APPEND'
                job = client.load_table_from_uri(['gs://bqload_python_source/distcp/employees_orc/000000_0','gs://bqload_python_source/distcp/employees_orc/000000_0_copy_1'], table_ref, job_config=job_config)

        # job is async operation so we have to wait for it to finish
                job.result()
        
        except Exception as e:
            conf.logger.error(f'{type(e).__name__} Exception Occured')
            print(type(e).__name__)
            raise e  


