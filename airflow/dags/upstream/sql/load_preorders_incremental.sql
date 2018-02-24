select * from `{{ var.value.preorders_src }}`
where load_datetime > timestamp('{{ ti.xcom_pull("last_update_preorders") }}')