# Purpose

The repo creates examples of scripts and python notebooks to easily play with and manupulate 
resources in gcs, bq usinh command line, python scripts and jupyter notebooks.

Will enable you to:
* view datasets
* view tables
* copy tables
* delete tables
* recover tables
* copy files gcs
* transfer data across regions
* suspend cronjobs in kubernetes cluster
* create cronjobs in kubernetes cluster
  

# Requirements

* Python 3.7+
* Git
* Google Cloud Platform Project
* Bigquery dataset, Cloud storage bucket and/or kubernetes cluster
* Google Cloud SDK installed with gsutil, kubectl and bq components
* Google Cloud SDK version 300.0.0 or above
* Have dataset and table owner/editor permissions and acl (access level control ) on writing and reading from GCS buckets