#!/bin/bash
name=swift-airflow

gcloud compute instances create $name \
--image airflow-server-image \
--image-project umg-dev \
--zone us-central1-c \
--machine-type n1-standard-4 \
--metadata-from-file startup-script=airflow-bootstrap.sh \
--scopes compute-rw,cloud-platform,logging-write,monitoring-write,service-control,service-management,sql-admin,storage-full