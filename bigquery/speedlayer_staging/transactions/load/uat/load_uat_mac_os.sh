#!/bin/bash

#delete transactions table. Will have to confirm from console
bq rm umg-swift:consumption_speedlayer_staging.transactions

#create new transactions table
bq mk --time_partitioning_type=DAY umg-swift:consumption_speedlayer_staging.transactions

#load daily partitions for the specified range
echo "Loading daily partitions from $1 to $2"
./partition_loop_mac_os.sh $1 $2 transactions_daily.sh

#load RTD partition with all transactions before first date in range
startPartitionTs=$(date -j -f "%Y-%m-%d" $1 "+%s")
offset1day=86400 #1 day (60*60*24)
rtdPartitionTs=$(($startPartitionTs-$offset1day))
rtdPartition=$(date -j -f "%s" $rtdPartitionTs "+%Y-%m-%d")
rtdPartitionNoDash=$(date -j -f "%s" $rtdPartitionTs "+%Y%m%d")

echo "Loading RTD transactions before $rtdPartition to 1900-01-01 partition"
./transactions_rtd.sh $rtdPartition $rtdPartitionNoDash


