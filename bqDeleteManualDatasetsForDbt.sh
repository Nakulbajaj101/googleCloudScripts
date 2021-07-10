#!/bin/bash

PROEJECT_ID=$1

declare -a datasets=("robert" "arturo" "michael" "avinash" "xiaoxi" "benjamin" "kelly" "svetlana" "nakul" "maranthony" "shivang")

for dataset in "${datasets[@]}"
do
   echo "dataset to be removed is $dataset"


   bq rm -r -f -d $PROEJECT_ID:$dataset
   # or do whatever with individual element of the array
done