#standardSQL
with projects as (select [cast(lp.project_id as string), cast(cp.project_id as string)] as project_id
from `{{ var.value.metadata_dst }}.product_{{ ds_nodash }}` cp
inner join `{{ var.value.metadata_dst }}.product_{{ yesterday_ds_nodash }}` lp
    on cp.product_id = lp.product_id
where cp.project_id <> lp.project_id
      and cast(cp.master_artist_id as string) not in (select master_artist_id from  `{{ var.value.metadata_dst }}.blacklist_artist`)
      and (datetime_diff(datetime(cp.product_release_date),
                         datetime('{{ ds }}'), DAY) > 28
           or datetime_diff(datetime(cp.project_release_date),
                         datetime('{{ ds }}'), DAY) > 28))

select distinct a.project_id
from
(select distinct project_id
from projects
cross join unnest(project_id) as project_id) a
left outer join (select * from `{{ var.value.metadata_dst }}.changeset_project_weekly`
                 where _partitiontime = timestamp('{{ ti.xcom_pull("set_context")["end_of_week"] }}')) cst
    on cst.project_id = a.project_id
where cst.project_id is null
