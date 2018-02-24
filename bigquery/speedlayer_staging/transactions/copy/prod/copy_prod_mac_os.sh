#!/bin/bash

#delete transactions table. Will have to confirm from console
bq rm umg-swift:consumption_speedlayer_staging.transactions

#create new transactions table
bq mk --time_partitioning_type=DAY umg-swift:consumption_speedlayer_staging.transactions

#load daily partitions for the specified range
echo "Loading daily partitions from $1 to $2"
./partition_loop_mac_os.sh $1 $2 transactions_daily.sh



