import logging
import pandas as pd
import subprocess
import pprint
from config import extract_project,import_project, import_bucket_uri,export_bucket_uri
from bigqueryTransferService import BigqueryTransferService
from cloudStorageTransferService import CloudStorageTransfer

# TODO: Need to create bqSchemaDev.py and bqSchemaStage.py as python files holding variables as dict objects of bq schemas
# TODO: Need to create viewDatasetsAndTables.py python files holding list of views datasets and tables to migrate

#from bqSchemaDev import schemas as devSchemas
#from bqSchemaStage import schemas as stageSchemas
#from viewDatasetsAndTables import viewDatasets, viewTables

logging.basicConfig(level=logging.INFO)


#Uncomment the code to get schemas for table, etc
"""
datasets = pd.read_csv("migrateStageDatasets.txt", header=0,skiprows=0)
print(datasets)
schemas = {}
for dataset in datasets['Datasets']:
    bq = BigqueryTransferService(extract_project, 'any',dataset)
    tables = bq.find_tables()
    schemas[dataset] = [{tbl:bq.get_schema(dataset,tbl)} for tbl in tables]

schemas = pprint.pformat(schemas)
with open ('schemas.txt', 'w') as file:
    file.write(schemas)

"""


datasets = [dt for dt in stageSchemas.keys()]
for dt in datasets:
    for config in stageSchemas[dt]:
        for table, schema in config.items(): 
            if (dt in viewDatasets) and (table in viewTables):

                logging.info(f"Cannot migrate {dt}.{table} since its a view")
            elif (dt in viewDatasets or dt not in viewTables) and (table not in viewTables):
                logging.info(f"Starting migration {dt}.{table}")
                #Copy tables to bucket

                bqExtract = BigqueryTransferService(extract_project,bucket_uri=export_bucket_uri + '/' + dt,dataset=dt)

                bqExtract.move_data_bq_gcs(table_id=table,table_location="EU",file_name=table, file_extension='json',compression="gzip")

#Moving tables to au bucket

logging.info("Moving data from eu to au region")
bqMove = CloudStorageTransfer(export_bucket_uri, import_bucket_uri)
bqMove.copy(mode="mv")


logging.info("All data moved across regions")










