import pandas as pd
import json
import logging
from config import extract_project,import_project, import_bucket_uri,export_bucket_uri
from bigqueryTransferService import BigqueryTransferService

# TODO: Need to create bqSchemaDev.py and bqSchemaStage.py as python files holding variables as dict objects of bq schemas
# TODO: Need to create viewDatasetsAndTables.py python files holding list of views datasets and tables to migrate

#from bqSchemaDev import schemas as devSchemas
#from bqSchemaStage import schemas as stageSchemas

#from viewDatasetsAndTables import viewDatasets, viewTables


logging.basicConfig(level=logging.INFO)


datasets = [dt for dt in stageSchemas.keys()]
for dt in datasets:
    for config in stageSchemas[dt]:
        for table, schema in config.items():

            if (dt in viewDatasets) and (table in viewTables):

                logging.info(f"Cannot migrate {dt}.{table} since its a view")
            elif (dt in viewDatasets or dt not in viewTables) and (table not in viewTables):
                logging.info(f"Starting migration {dt}.{table}")
                #Copy tables to bucket

                bqImport = BigqueryTransferService(extract_project,bucket_uri=import_bucket_uri + '/' + dt,dataset=dt)

                bqImport.move_data_gcs_new_bq_table(table_id=table,config='json', schema=schema)

#Moving tables to au bucket

logging.info("All data extracted and moved into au")













