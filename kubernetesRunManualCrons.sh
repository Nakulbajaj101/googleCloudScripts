#!/bin/bash

set -euo pipefail

GCP_CLUSTER=$1
GCP_PROJECT=$2
NAMESPACE=$3
CRON_NAME=$4

TIMESTAMP=`date +"%s"`

gcloud container clusters get-credentials $GCP_ENV-svpc-app --region australia-southeast1 --project $GCP_PROJECT
kubectl create job -n -n $NAMESPACE $CRON_NAME-$TIMESTAMP --from=cronjob/$CRON_NAME
