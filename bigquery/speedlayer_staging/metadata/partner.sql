--standard SQL

with partner as (
  select a.master_account_code,
  a.master_account_name,
  array_agg(distinct a.sales_country_code) as countries,
  array_agg(distinct r.consumption_region_code) as regions
  from `umg-marketing.metadata.master_account` a
  left outer join `umg-marketing.metadata.region` r on a.sales_country_code = r.sales_country_code
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
else 0 end as priorityPartner
from partner p
left outer join `umg-dev.consumption_speed_layer_staging.priority_partners` pp 
  on cast(p.master_account_code as string) = pp.partner_id
order by priorityPartner desc


