#!/bin/bash
src_agg_dataset='umg-swift.consumption_speedlayer_aggregation'
dst_agg_dataset='umg-swift:consumption_speedlayer_aggregation_v2'
table=$1

while read shard; do
  echo ${shard}
 
  #deleting existing shard table at destination if exists
  bq --headless rm "${dst_agg_dataset}.${table}_${shard}"

  #create new shard table at destination
  bq mk --time_partitioning_type=DAY "${dst_agg_dataset}.${table}_${shard}"

  #get list of partitions from the source
  ./get_partitions.sh $1 ${shard} | tail -n +3  > partitions/$1_${shard}.txt
  
  while read partition_id; do

    #convert partition_id to dashed format 
    ts_partition=$(date -j -f "%Y%m%d" ${partition_id} "+%s")
    partition_id_dashed=$(date -j -f "%s" ${ts_partition} "+%Y-%m-%d")

    bq --nosync query --batch --allow_large_results --replace --nouse_legacy_sql  --parameter datePartition:STRING:${partition_id_dashed} --destination_table ${dst_agg_dataset}.${table}_${shard}\$${partition_id} `cat sql/${table}.sql  | sed -e "s/#src_agg_dataset#/$src_agg_dataset/" | sed -e "s/#table#/$table/" | sed -e "s/#shard#/$shard/"`
  
  done < partitions/$1_${shard}.txt

  rm partitions/$1_${shard}.txt
done < shards/${table}.txt