import os
import Constants
import pandas as pd
from AppLogger import get_logger
from pyhive import hive
from PropertyUtility import PropertyUtility
class ValidationUtility:
   logger = get_logger(__name__)
   def generateValidationReport(self):
      logger.info("Generate Validation Report started")
      pu=PropertyUtility()
      databaseref=pu.getValue(Constants.SECTION,Constants.SOURCE_DB)
      conn=hive.Connection(host="localhost",database=databaseref)
      #destdata=conf.destination_db
      #desttable=conf.destination_table
      project_id=pu.getValue(Constants.SECTION,Constants.PROJECT_ID)
      sa_keypath=pu.getValue(Constants.SECTION,Constants.PROJECT_PATH)+pu.getValue(Constants.SECTION,Constants.SA_KEYPATH)
      validation_bucket_path=pu.getValue(Constants.SECTION,Constants.VALIDATION_BUCKET_PATH)
      os.system(f'export PSO_DV_CONFIG_HOME={validation_bucket_path}')
      logger.info("Connecting to hive.....")
      HiveConn=f"data-validation connections add --connection-name {databaseref}_HIVEConn Impala --host localhost --port 10000 --database {databaseref} --auth-mechanism PLAIN"
      BQConn=f"data-validation connections add --connection-name {databaseref}_BQconn BigQuery --project-id {project_id} --google-service-account-key-path {sa_keypath}"
      #tables_list=open(f"../output/{databaseref}_tables.txt").read().splitlines()
      tables_list = pd.read_csv(f"../output/{databaseref}_tables.txt", header=None)[0].tolist()
      print(tables_list, 'TABLES_LIST_IN_VALIDATION')
      os.system(HiveConn)
      os.system(BQConn)
      for table_ref in tables_list:
          column_names=pd.read_sql(f"show columns in {table_ref}",conn)
          print(column_names, 'listprint')
          #comma_seperated_field=x+',' for x in list(column_names)
          #(lambda x: for x in list(column_names): x+',')
          #comma_seprated_field=func(list(column_names))
          #field=",".join(column_names[0])
          
          #field=pd.concat(column_names, sep=',')
          field=''
          print(column_names)
          for column_name in column_names.index:
                field+=column_names['field'][column_name]+","
          field=field[:-1]
          print(field)
          DataValidation=f"data-validation validate column -sc  {databaseref}_HIVEConn -tc {databaseref}_BQconn -tbls {databaseref}.{table_ref}={databaseref}.{table_ref} -bqrh {project_id}.pso_data_validator.results -sa {sa_keypath}"
          #print(DataValidation)
          os.system(DataValidation)
      logger.info("Generate Validation Report completed")
