SELECT distinct t.usage_group, t.usage_type, fp.free_paid
FROM `umg-swift.consumption.combined_transactions` t
left outer join `umg-swift.consumption_speedlayer_staging.free_paid_mapping` fp 
  on lower(ltrim(rtrim(t.usage_group))) = lower(ltrim(rtrim(fp.usage_group))) 
  and lower(ltrim(rtrim(t.usage_type))) = lower(ltrim(rtrim(fp.usage_type)))
WHERE load_datetime <= timestamp('2017-12-24 07:38:16') 
and fp.free_paid is null
order by t.usage_group, t.usage_type