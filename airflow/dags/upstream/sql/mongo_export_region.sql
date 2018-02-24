#standardSQL
with regions as (select region_id,
region_name,
region_order,
array_agg(distinct sst_country_code) as countries 
from  `{{ var.value.metadata_dst }}.sst_country_region`
group by region_id, region_name, region_order),

partners as (select r.region_id,
array_agg(distinct p.master_account_code) as partners
from  (select distinct region_id, sst_country_code, sales_country_code
       from `{{ var.value.metadata_dst }}.sst_country_region`) r
left outer join `{{ var.value.metadata_src }}.master_account` p on r.sales_country_code = p.sales_country_code
group by r.region_id )

SELECT r.region_id as _id, 
r.region_name as name,
array(select el from unnest(r.countries) as el WHERE el IS NOT NULL) as countries,
array(select el from unnest(p.partners) as el WHERE el IS NOT NULL) as partners, 
r.region_order as `order`,
current_timestamp() as load_datetime
FROM regions r
inner join partners p on r.region_id = p.region_id
where r.region_id <> 38
order by `order`
