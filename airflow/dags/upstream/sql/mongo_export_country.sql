#standardSQL
with segments as (select c.sst_country_code,
array_agg(distinct ss.sap_segment_code) as sap_segments
from (select distinct sst_country_code
      from  `{{ var.value.metadata_dst }}.sst_country_region`) c
left outer join ( select * from `{{ var.value.metadata_dst }}.sap_segment` order by `order`) ss on c.sst_country_code = ss.sst_country_code 
group by c.sst_country_code ),

partners as (select c.sst_country_code,
array_agg(distinct p.master_account_code) as partners
from (select distinct sst_country_code, sales_country_code
      from  `{{ var.value.metadata_dst }}.sst_country_region`) c
left outer join `{{ var.value.metadata_src }}.master_account` p on c.sales_country_code = p.sales_country_code
group by c.sst_country_code)

select c.sst_country_code as _id,
    c.sst_country_name as name,
    array(select el from unnest(seg.sap_segments) as el WHERE el IS NOT NULL) as sap_segments,
    array(select el from unnest(par.partners) as el WHERE el IS NOT NULL) as partners,
    c.country_order as `order`,
    current_timestamp() as load_datetime
from (select distinct sst_country_code, sst_country_name, country_order
      from  `{{ var.value.metadata_dst }}.sst_country_region`) c
inner join segments seg on c.sst_country_code = seg.sst_country_code
inner join partners par on c.sst_country_code = par.sst_country_code
order by `order`