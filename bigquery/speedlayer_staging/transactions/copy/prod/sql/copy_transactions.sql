#standardSQL
select * from `umg-swift.consumption_speedlayer_staging.transactions_v2`
where _partitiontime = timestamp(@datePartition)
