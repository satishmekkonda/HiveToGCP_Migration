from google.cloud import bigquery
import pandas as pd
from AppLogger import get_logger
import Constants
from Connections import ConnUtil as ConnUtil

class ViewUtil:
    logger = get_logger(__name__)
    table_names=[]
    def CreateView(self):
        conf = Constants.variable
        try:
            self.table_names = pd.read_csv(f"../output/{conf.source_db}_tables.txt", header=None)[0].tolist()
            print(self.table_names, 'TablesFound')
            print(conf.project_id)
            client=ConnUtil.BQConn(conf.project_id)
            project = conf.project_id
            print('Project', project)
            dataset_name =conf.source_db
            print('DatasetName', dataset_name) 
            for table in self.table_names:
                logger.info(f"connecting to Bigquery")
                print('Table', table)
                view_id = table+'_view'
                view = bigquery.Table(f'{project}.{dataset_name}.{view_id}')
                print('ViewName', view)
                create_view = """ 
                Select
                MD5(CAST(insert_time as string)) as insert_time,
                FROM   `{}.{}.{}` """
                view.view_query = create_view.format(client.project, dataset_name, table)
                view = client.create_table(view)
                print('View Created')
            
        
        except Exception as e:
            logger.error(f'{type(e).__name__} Exception Occured')
            print(type(e).__name__)
            raise e  


