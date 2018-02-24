select distinct  country_subdivision_code as _id, country_subdivision_name as name, sales_country_code as country
from
(select distinct  sales_country_code, country_subdivision_code, country_subdivision_name
from `umg-marketing.consumption.transactions_apple_music`
where country_subdivision_code <> ''
union all
select distinct  sales_country_code, country_subdivision_code, country_subdivision_name
from `umg-marketing.consumption.transactions_spotify`
where country_subdivision_code <> '')