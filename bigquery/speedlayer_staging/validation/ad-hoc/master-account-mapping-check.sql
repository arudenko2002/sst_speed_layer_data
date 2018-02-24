SELECT distinct t.sub_account_code, t.sales_country_code, ma.master_account_code
FROM `umg-swift.consumption.combined_transactions` t
left outer join `umg-swift.metadata.master_account` ma
  on t.sub_account_code = ma.sub_account_code 
  and t.sales_country_code = ma.sales_country_code 
WHERE t.load_datetime <= timestamp('2017-12-24 07:38:16') 
and ma.master_account_code is null
order by t.sub_account_code, t.sales_country_code