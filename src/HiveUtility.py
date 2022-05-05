import Constants
import os
import pandas as pd
from PropertyUtility import PropertyUtility
from AppLogger import get_logger




class HiveUtility:
    logger = get_logger(__name__)

    def generateDistCP(self):
        
        logger.info("GenerateDistCP started")
        pu=PropertUtility()
        database=pu.getValue(Constants.SECTION,Constants.SOURCE_DB)
        tables=pu.getValue(Constants.SECTION,Constants.DB_TABLE)
        project_path=pu.getValue(Constants.SECTION,Constants.PROJECT_PATH)
        source_bucket=pu.getValue(Constants.SECTION,Constants.SOURCE_BUCKET)
        target_bucket=pu.getValue(Constants.SECTION,Constants.TARGET_BUCKET)
        distcp_list=[]

        try:
            if tables[0]=='ALL':
                distcp_command = f'hadoop distcp hdfs://{source_bucket}/{database}.db/* gs://{target_bucket}/{database}.db/'
                distcp_list.append(distcp_command)
                distcp_list=pd.DataFrame(distcp_list)
                distcp_list.to_csv(f'{project_path}/output/distcp_{database}.txt', index=False, header=False)
                logger.info(f"All tables distcp generated")

            else:
                distcp_list=[]
                for table in tables:
                    command = f'hadoop distcp hdfs://{source_bucket}/{database}.db/{table}/* gs://{target_bucket}/{database}.db/{table}/'
                    distcp_list.append(command)
                distcp_list=pd.DataFrame(distcp_list)
                distcp_list.to_csv(f'{project_path}/output/distcp_generate.txt', index=False, header=False)
                logger.info(f"Given tables distcp generated")


        except Exception as e:
            logger.error(f'{type(e).__name__} Exception Occured')
            print(type(e).__name__)
            raise e

    def executeDistCP(self):
        #conf=Constants.variables()
        logger.info(f"ExecuteDistCP started")
        try:
            df = pd.read_csv(f'{project_path}/output/distcp_{database}.txt', header=None)
            df[0].apply(lambda x: os.system(x))
            logger.info(f'DistCP Executed Successfully')

        except Exception as e:
            logger.error(f'{type(e).__name__} Exception Occured')
            print(type(e).__name__)
            raise e
