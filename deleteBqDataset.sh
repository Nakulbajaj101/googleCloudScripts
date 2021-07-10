#!/bin/bash
PROJECT_ID=$1
DATASET_ID=$2

echo "Removing dataset ${PROJECT_ID}:${DATASET_ID}with all tables"

 #-f is for force, -r for all tables in dataset, -d for dataset

bq rm -r -f -d $PROJECT_ID:$DATASET_ID