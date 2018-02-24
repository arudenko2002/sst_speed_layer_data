select distinct partner_report_date
from
(
  select distinct partner_report_date
  from `{{ var.value.transactions_src }}`
  where load_datetime > timestamp('{{ ti.xcom_pull("last_update_timestamp") }}')
        and load_datetime < timestamp_add(timestamp('{{ ts }}'), interval 24 hour)
  union all
  select distinct parse_date('%Y%m%d', partner_report_date) as partner_report_date
  from `{{ var.value.preorders_src }}`
  where load_datetime > timestamp('{{ ti.xcom_pull("last_update_timestamp") }}')
        and load_datetime < timestamp_add(timestamp('{{ ts }}'), interval 24 hour)
)