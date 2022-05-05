import sys
import HiveUtility
import Constants
import GCPUtility
import ValidationUtility
from AppLogger import get_logger
import ViewUtility
import os
import PropertyUtility

class AppRunner:
    if __name__ == '__main__':

        var=Constants.variables()
        logger = get_logger(__name__)
        print(var.source_db)
        print(var.db_table)
        project_path=sys.argv[1]
        execution_type=sys.argv[2]
        print(sys.argv)
        print(project_path, execution_type, 'Apprunner Arguments')
        Hive=HiveUtility.HiveUtil()
        GCP=GCPUtility.GCPUtil()
        Validate=ValidationUtility.ValidationUtil()
        prop=PropertyUtility.PropertyUtil()
        #View=ViewUtility.ViewUtil()

        if project_path.endswith('\\') or project_path.endswith('/'):
            pass
        else:
            project_path = project_path + '/'

        prop.setValue('Common','project_path',project_path)
        #os.system(f'export project_path={project_path}')
        
        if  execution_type=='GENERATE_DISTCP':
            logger.info(f"GENERATE_DISTCP has matched")
            Hive.GenerateDistCP()
            print('Generating DistCP Success')

        elif execution_type=='EXECUTE_DISTCP':
            logger.info(f"EXECUTE DISTCP has matched")
            Hive.ExecuteDistCP()
            print('Executing DistCP Success')
    
        elif execution_type=='GENERATE_BQLOAD':
            logger.info(f"GENERATE_BQLOAD has matched")
            GCP.GenerateBQLoad()
            print('Generating BQLoad Success')
            print(GCP.table_names)
   
        elif execution_type=='EXECUTE_BQLOAD':
            logger.info(f"EXECUTE_BQLOAD has matched")
            GCP.ExecuteBQLoad()
            print('Executing BQLoad Success')

        elif execution_type=='VALIDATION_REPORT':
            logger.info(f"VALIDATION_REPORT has matched")    
            Validate.GenerateValidationReport()
            print('Validation Successful')
    
        #elif arg=='CREATE_VIEW':
         #   log.logger.info(f"Create View In Progress")
          #  View.CreateView()
           # print('View Creation Done Successfully')
        
        else:
            print('Unrecognized Argument Passed')
            logger.info(f"Argument doesn't match")
