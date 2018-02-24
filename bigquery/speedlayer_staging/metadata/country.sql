#standardSQL
with segments as (select c.sst_country_code,
array_agg(distinct lp.sap_segment_code) as sap_segments
from (select distinct sst_country_code, sales_country_code
      from  `umg-swift.consumption_speedlayer_staging.sst_country_region`) c
left outer join `umg-marketing.metadata.local_product` lp on c.sales_country_code = lp.sales_country_code and
                lp.sales_country_code in ('US','GB','DK','CA','SE','JP','NO','BE','FR','NL','DE','IE') and
                lp.sap_segment_code <> '-'
group by c.sst_country_code ),

partners as (select c.sst_country_code,
array_agg(distinct p.master_account_code) as partners
from (select distinct sst_country_code, sales_country_code
      from  `umg-swift.consumption_speedlayer_staging.sst_country_region`) c
left outer join `umg-marketing.metadata.master_account` p on c.sales_country_code = p.sales_country_code
group by c.sst_country_code)

select c.sst_country_code as _id,
    c.sst_country_name as name,
    array(select el from unnest(seg.sap_segments) as el WHERE el IS NOT NULL) as sap_segments,
    array(select el from unnest(par.partners) as el WHERE el IS NOT NULL) as partners,
    c.country_order as `order`
from (select distinct sst_country_code, sst_country_name, country_order
      from  `umg-swift.consumption_speedlayer_staging.sst_country_region`) c
inner join segments seg on c.sst_country_code = seg.sst_country_code
inner join partners par on c.sst_country_code = par.sst_country_code
order by `order`