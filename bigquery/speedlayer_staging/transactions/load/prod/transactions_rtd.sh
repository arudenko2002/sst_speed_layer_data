#!/bin/bash
partitionDate=$1
bq --nosync query --batch --allow_large_results --replace --nouse_legacy_sql --parameter datePartition:STRING:$partitionDate --destination_table umg-swift:consumption_speedlayer_staging.transactions_current\$19990101 "$(cat sql/transactions_rtd.sql)"