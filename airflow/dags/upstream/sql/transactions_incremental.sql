select * from `{{ var.value.transactions_src }}`
where load_datetime > timestamp('{{ ti.xcom_pull("last_update_transactions") }}')