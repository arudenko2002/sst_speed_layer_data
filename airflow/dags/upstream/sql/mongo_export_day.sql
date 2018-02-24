#standardSQL
select day_id as _id,
day_name,
week,
month,
month_name,
quarter,
year,	
accounting_month,
global_week,
global_year
from `{{ var.value.metadata_src }}.day`