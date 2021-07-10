#!/bin/bash
project_id=$1

#list datasets
for dataset in $(bq ls -d --project_id=${project_id} --format=prettyjson | jq '.[] | select(.location == "EU") .datasetReference .datasetId' | tr -d '"');  
    do echo "  " 
    echo DATASET $dataset
    bq ls -n 1000 --dataset_id=$dataset | awk {'print $1'} 
    echo TotalTables
    bq ls -n 1000 --dataset_id=$dataset --format=prettyjson | jq '.[] | .tableId ' | wc -l; 
done 
