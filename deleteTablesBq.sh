#!/bin/bash
project=$1
cat datasetsDelete.txt | while read dataset ; 
    do echo $dataset;
    if [ $dataset != "Datasets" ]; then

    for table in $(bq ls -n 1000 --dataset_id=$dataset --project_id=$project | awk '{print $1}');
        do bq rm -f -t $project:$dataset.$table;
    done;

    #Deleting dataset
    bq rm -r -f -d $project:$dataset;
    #Recreating dataset in australia
    bq --location=australia-southeast1 mk -d $project:$dataset;
    fi;
done;

