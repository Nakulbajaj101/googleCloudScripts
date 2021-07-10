#!/bin/bash 

GCP_CLUSTER=$1
GCP_PROJECT=$2
NAMESPACE=$3
CRON_NAME=$4

set -euo pipefail

gcloud container clusters get-credentials $GCP_CLUSTER --region australia-southeast1 --project $GCP_PROJECT

kubectl patch cronjobs -n $NAMESPACE $CRON_NAME -p '{"spec" : {"suspend" : true }}'
