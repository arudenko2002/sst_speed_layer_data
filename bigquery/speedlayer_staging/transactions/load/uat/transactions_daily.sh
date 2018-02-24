#!/bin/bash
partitionDate=$1
bq --nosync query --batch --allow_large_results --replace --nouse_legacy_sql --parameter datePartition:STRING:$partitionDate --destination_table umg-swift:consumption_speedlayer_staging.transactions\$$2 "$(cat sql/transactions_daily.sql)"