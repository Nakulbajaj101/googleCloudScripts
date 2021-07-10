import os
import json
from google.cloud import bigquery

project_id="" #Project id for your project

files = [f for f in os.listdir(os.getcwd()+'/views')]
client = bigquery.Client(f"{project_id}")
for file in files:
    query = ""
    with open(os.getcwd() + '/views/' + file, 'r') as jsonFile:
        view = json.load(jsonFile)
        view_id = view['id'].replace(':','.')
        vw = bigquery.Table(view_id)
        vw.view_query = view['view']['query']
        try:
            client.create_table(vw)
        except Exception as e:
            print(e)

