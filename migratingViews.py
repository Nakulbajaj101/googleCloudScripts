import subprocess
from viewDatasetsAndTables import viewDatasetsTableMapping as vdtm
import os

project = 'anz-insto-data-analytics-dev'

views_dir = os.getcwd() + "/views"
if not os.path.exists(views_dir):
    os.mkdir(views_dir)

for dt in vdtm.keys():
    print(dt)
    for view in vdtm[dt]:
        command = f"""bq show --format=prettyjson --view {project}:{dt}.{view} > {views_dir}/{view}.json"""
        process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        output, error = process.communicate()
        print("output: {}" .format(output.decode('utf-8')) , "error: {}" .format(error))











