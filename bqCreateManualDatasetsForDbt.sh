#!/bin/bash

PROEJECT_ID=$1

declare -a datasets=("robert" "arturo" "michael" "avinash" "xiaoxi" "benjamin" "kelly" "svetlana" "nakul" "maranthony" "shivang")

for dataset in "${datasets[@]}"
do
   echo "dataset to be created is $dataset"


   bq --location="australia-southeast1" mk \
    --dataset \
    --default_table_expiration 7200 \
    --description "$dataset's dbt dataset" \
    $PROEJECT_ID:$dataset
   # or do whatever with individual element of the array
done