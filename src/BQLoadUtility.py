from google.cloud import storage
import pandas as pd
import Logger
#required bq load
def BQLoadCommand():
    log = Logger.LogUtil()
    log.logger.info("Generating BQLoadCommand started")
    pd.set_option("display.max_colwidth",None)
    storage_client = storage.Client()

    #loaction for databases
    target_gcs_bucket='hive-database-bqload-template'

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
        bqload_command=f'bq load --source_format=ORC --hive_partitioning_mode=AUTO --hive_source_uri_prefix={bucket_name}/{table_path} --time_partitioning_field=cda_year --time_partitioning_type=DAY source_db.table gs://{bucket_name}/{table_path}*'
        bqload.append(bqload_command)
    bqload_df=pd.DataFrame(bqload)
    bqload_df.to_csv(f'{conf.project_path}/output/bqload_db_commands.txt', index=False, header=False)  #put conf.source_db in place of database 1
    log.logger.info(f"BQLoadCommand saved in {conf.project_path}/output/bqload_db_commands.txt")




