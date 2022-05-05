from google.cloud import bigquery
import pandas as pd
import Constants
import AppRunner as App
from AppLogger import get_logger
from DatasetUtility import DatasetUtility
from Connections import ConnUtil as ConnUtil
from BQLoadUtility import BQLoadCommand
from pyhive import hive
from PropertyUtility import PropertyUtility

class GCPUtility:
    logger = get_logger(__name__)
    table_names = []
    pu=PropertyUtility()
    source_db=pu.getValue(Constants.SECTION,Constants.SOURCE_DB)
    project_id=pu.getValue(Constants.SECTION,Constants.PROJECT_ID)
    db_table=pu.getValue(Constants.SECTION,Constants.DB_TABLE)
    project_path=pu.getValue(Constants.SECTION,Constants.PROJECT_PATH)
    def writeTablesToTextFile(self, source_db,  tables_names):
        logger.info(f"Writing tables to text files")
        table_list_filename=f'../output/{source_db}_tables.txt'
        tables_list=pd.DataFrame(tables_names)
        tables_list.to_csv(table_list_filename, index=False, header=False)   
  
    def generateBQLoad(self):
        logger.info(f"GenerateBQLoad started")
        try:
            if db_table[0]=='ALL':
                logger.info(f"connecting to hive")
                conn=hive.Connection(host='localhost', database=source_db)
                query='SHOW TABLES'
                result=pd.read_sql(query,conn)
                result.iloc[:,0].to_csv(f'../output/{source_db}_tables.txt', header=False, index=False)

            else:
                GCPUtil.table_names = db_table
                self.writeTablesToTextFile(source_db, self.table_names)
            # BQLoadCommand()
            storage_client = storage.Client()
            #loaction for databases
            target_gcs_bucket='hive-database-bqload-template' #Add target bucket variable
            #prefix argument will contain the folderpath inside bucket to the surce database
            prefix_path=f'{source_db}/'   #add conf.source_db in place of database1
            buckets=storage_client.list_blobs(bucket_name,prefix=prefix_path,delimiter='/') 
            for bucket in buckets:
                pass
            table_path_list=[]
            for prefix in buckets.prefixes:
                #print(prefix)
                table_path_list.append(prefix)

            bqload=[]
            for table_path in table_path_list:
                bqload_command=f'bq load --source_format=ORC --hive_partitioning_mode=AUTO --hive_source_uri_prefix={bucket_name}/{table_path} --time_partitioning_field=cda_year --time_partitioning_type=DAY source_db.table gs://{bucket_name}/{table_path}*' #add variab;e for time partitioning field
                bqload.append(bqload_command)
            bqload_df=pd.DataFrame(bqload)
            bqload_df.to_csv(f'{project_path}/output/bqload_db_commands.txt', index=False, header=False)  #put conf.source_db in place of database 1
            logger.info(f"BQLoadCommand saved in {project_path}/output/bqload_db_commands.txt")
            logger.info(f"GenerateBQLoad  finished")

        except Exception as e:
            logger.error(f'{type(e).__name__} Exception Occured')
            print(type(e).__name__)
            raise e            


    def executeBQLoad(self):
        logger.info(f"ExecuteBQLoad started")
        du=DatasetUtility()
        try:
            client=ConnUtil.BQConn(project_id)
            datasets = list(client.list_datasets())
            datasets=list(map(lambda x : x.dataset_id , datasets))
            if source_db not in datasets:
                dataset = client.create_dataset(f'{project_id}.{source_db}', timeout=30)
            du.runLoadJob(source_db,project_path,project_id)
            
        except :
            logger.error(f'{type(e).__name__} Exception Occured')
            print(type(e).__name__)
            raise e






