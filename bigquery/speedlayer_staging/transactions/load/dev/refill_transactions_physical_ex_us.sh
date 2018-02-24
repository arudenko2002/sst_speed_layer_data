#!/bin/bash
partitionDate=$1
bq --nosync query --batch --allow_large_results --append_table --nouse_legacy_sql --parameter datePartition:STRING:$partitionDate --destination_table umg-dev:consumption_speed_layer_staging.transactions_v3\$$2 "$(cat sql/rebuild_transactions_physical_ex_us.sql)"