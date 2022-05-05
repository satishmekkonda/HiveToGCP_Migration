from google.cloud import bigquery
from pyhive import hive
import pandas as pd
from AppLogger import get_logger
import Constants
from Connections import ConnUtil as ConnUtil
from google.cloud.bigquery.external_config import HivePartitioningOptions

class DatasetUtility:
    logger = get_logger(__name__)
    table_names=[]
    def runLoadJob(self,source_db,project_path,project_id):
        try:
            self.table_names = pd.read_csv(f'{project_path}/output/{source_db}_tables.txt', header=None)[0].tolist()
            client=ConnUtil.BQConn(project_id)
            conn=hive.Connection('localhost',database=source_db)
            for table in self.table_names:
                logger.info(f"connecting to Bigquery")
                ###
                desc_query=f'describe {table}'
                result_df=list(pd.read_sql(desc_query,conn)['col_name'])
                find_partition = list(filter(lambda x: ('Partition' in x), result_df))
                partition_index = result_df.index(find_partition[0])
                partition_field=result_df[partition_index+2]
                print(f"{partition_field} partition field")

                ###
                table_ref = client.dataset(source_db).table(table)
                hive_config = HivePartitioningOptions()
                hive_config.mode = 'AUTO'
                hive_config.source_uri_prefix = f'gs://demo-migration-cb/{source_db}.db/{table}' #Add target bucket variable
                #RangePartition = bigquery.RangePartitioning(range_=bigquery.PartitionRange(start='1500', interval='1', end='3000'), field=partition_field)
                time_partition = bigquery.TimePartitioning(type_ = 'DAY', field = 'dob')
                job_config = bigquery.LoadJobConfig(source_format = 'ORC',
                hive_partitioning = hive_config,
                time_partitioning = time_partition,
                                 autodetect = False,
                                write_disposition = 'WRITE_APPEND')
                job = client.load_table_from_uri(f'gs://demo-migration-cb/{source_db}.db/{table}/*', table_ref, job_config=job_config) #add target bucket variable
                
            # job is async operation so we have to wait for it to finish
                job.result()
                logger.info(f"ExecuteBQLoad finished")
        
        except Exception as e:
            logger.error(f'{type(e).__name__} Exception Occured')
            print(type(e).__name__)
            raise e  
