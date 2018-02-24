select *
,case when sap_segment_name = 'Other' then 10000
      else (rank() over (partition by sst_country_code order by sap_segment_name asc)) * 10 end as `order`
from
(
select t.sst_country_code as sst_country_code
,t.sst_country_name as sst_country_name
,t.sap_segment_code as sap_segment_code
,sum(t.units) as units
,case when m.sap_segment_name is null then t.sap_segment_name else m.sap_segment_name end as sap_segment_name
from
(
SELECT
c.sst_country_code as sst_country_code
,c.sst_country_name as sst_country_name
,case when p.sap_segment_code = '' or p.sap_segment_code = '-' or p.sap_segment_code is null or (p.sap_segment_code = 'CAN01' and c.sst_country_code = 'GB')
      then 'OTH' else sap_segment_code end as sap_segment_code 
,p.sap_segment_name as sap_segment_name
,sum(t.units) as units
FROM
(select product_id, sales_country_code, sum(units) as units
from `{{ var.value.transactions_src }}`
group by product_id, sales_country_code) t
left join
(
select product_id, sales_country_code
,sap_segment_code  
,sap_segment_name
from `{{ var.value.metadata_src }}.local_product`
group by product_id, sales_country_code, sap_segment_code, sap_segment_name) p
on t.product_id = p.product_id and t.sales_country_code = p.sales_country_code
join
(select sales_country_code, sst_country_code, sst_country_name
from `{{ var.value.metadata_dst }}.sst_country_region`
where sst_country_code in ('US','GB','DK','CA','SE','JP','NO','BE','FR','NL','DE','IE')
group by sales_country_code, sst_country_code, sst_country_name) c
on t.sales_country_code = c.sales_country_code
group by c.sst_country_code, c.sst_country_name, p.sap_segment_code, p.sap_segment_name
) t
left join `{{ var.value.metadata_dst }}.sap_segment_code_to_name` m
on t.sap_segment_code = m.sap_segment_code
group by sst_country_code, sst_country_name, sap_segment_code, sap_segment_name
)