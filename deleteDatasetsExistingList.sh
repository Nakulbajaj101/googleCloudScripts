#!/bin/bash
project=$1
cat datasetsDelete.txt | tr -d '"'| while read dataset ; 
    do echo $dataset;
    bq rm -r -f -d $project:$dataset;
done;