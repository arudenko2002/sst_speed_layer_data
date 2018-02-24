#!/bin/bash
name=umg-swift-cloud-sql
tier=db-n1-standard-4
zone=us-central1-c
password=pass@word1

# create master instance
gcloud sql instances create $name \
--tier=$tier \
--backup-start-time=12:00 \
--enable-bin-log \
--failover-replica-name=$name-failover \
--storage-auto-increase \
--storage-type=SSD \
--gce-zone=$zone

# set root password
gcloud sql users set-password root % --instance $name --password $password

# create aiflow database
gcloud sql databases create airflow --instance $name