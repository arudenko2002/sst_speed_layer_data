select ma.master_account_code as _id,
ma.master_account_name as name,
case when pp.partner_id is null then 0 else 1 end as priority,
ifnull(bq.partner_report_date, t.partner_report_date) as last_report_date_bq,
format_timestamp('%Y-%m-%dT%H:%M:%S', ifnull(bq.load_datetime, t.load_datetime)) as load_datetime_bq,
t.partner_report_date as last_report_date_bt,
format_timestamp('%Y-%m-%dT%H:%M:%S', t.load_datetime) as load_datetime_bt
from (select master_account_code, 
      max(partner_report_date) as partner_report_date,
      max(load_datetime) as load_datetime
      from `{{ var.value.transactions_dst}}`
      group by master_account_code) t
left outer join (SELECT master_account_code, 
                   max(partner_report_date) as partner_report_date, 
                   max(load_datetime) as load_datetime 
                   FROM `{{ var.value.metadata_dst }}.bt_flag` 
                   WHERE master_account_code IS NOT NULL
                   GROUP BY master_account_code) bq on t.master_account_code = cast(bq.master_account_code as string)
inner join (select distinct master_account_code, master_account_name
            from `{{ var.value.metadata_dst }}.master_account`) ma 
            on t.master_account_code = cast(ma.master_account_code as string)
left outer join `{{ var.value.metadata_dst }}.priority_partners` pp 
            on cast(ma.master_account_code as string) = pp.partner_id
order by priority desc,
name