import pyhive as hive
from google.cloud import bigquery

class ConnUtil:

    def HiveConn(host, database):
        return hive.Connection(host=host, database=database) 
    
    def BQConn(project):
        return bigquery.Client(project=project)
