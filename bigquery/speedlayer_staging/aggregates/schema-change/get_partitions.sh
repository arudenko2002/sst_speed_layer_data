#!/bin/bash
bq query --max_rows 1000 --format csv  "SELECT partition_id \
           FROM [umg-swift:consumption_speedlayer_aggregation.$1_$2\$__PARTITIONS_SUMMARY__] \
           ORDER BY partition_id"