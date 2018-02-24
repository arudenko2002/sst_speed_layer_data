#standardSQL
with partner as (
  select a.master_account_code,
  a.master_account_name,
  array_agg(distinct r.sst_country_code) as countries,
  array_agg(distinct r.region_id) as regions
  from `{{ var.value.metadata_src }}.master_account` a
  left outer join (select distinct sst_country_code, sales_country_code, region_id
                   from  `{{ var.value.metadata_dst }}.sst_country_region`) r on a.sales_country_code = r.sales_country_code
  group by a.master_account_code,
  a.master_account_name)

select p.master_account_code as _id,
p.master_account_name as name,
array(select el
          from unnest(countries) as el
          where el is not null
            and el <> '') as countries,
array(select el
          from unnest(regions) as el
          where el is not null
            and cast(el as string) <> '') as regions,
case when pp.partner_id is not null then 1
else 0 end as priorityPartner,
current_timestamp() as load_datetime
from partner p
left outer join `{{ var.value.metadata_dst }}.priority_partners` pp 
  on cast(p.master_account_code as string) = pp.partner_id
order by priorityPartner desc