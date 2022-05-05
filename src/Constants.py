import PropertyUtility as prop
import os

class variables:
    var=prop.PropertyUtil()
    DEFAULT_PROJECT_PATH='/home/satishmekkonda/CoBaGcpPoc/'
    #project_path=os.getenv('project_path')
    project_path=var.getValue('Common', 'PROJECT_PATH')
    project_id=var.getValue('Common', 'PROJECT_ID')
    source_db=var.getValue('Common', 'SOURCE_DB')
    #destination_db=var.getValue('Common', 'DESTINATION_DB')
    #destination_table=var.getValue('Common', 'DESTINATION_TABLE')
    db_table=var.getValue('Common', 'DB_TABLE').split(',')
    config_section=var.getValue('Common', 'CONFIG_SECTION')
    sa_keypath=var.getValue('Common', 'SA_KEYPATH')
    credentials_json_key=var.getValue('Common', 'CREDENTIALS_JSON_KEY')
    bucket_path=var.getValue('Common', 'BUCKET_PATH')
    validation_bucket_path=var.getValue('Common', 'VALIDATION_BUCKET_PATH')


