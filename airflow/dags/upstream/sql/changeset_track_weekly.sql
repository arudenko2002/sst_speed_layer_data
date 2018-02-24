#standardSQL
with tracks as (select [cast(lp.master_track_id as string), cast(cp.master_track_id as string)] as master_track_id
from `{{ var.value.metadata_dst }}.product_{{ ds_nodash }}` cp
inner join `{{ var.value.metadata_dst }}.product_{{ yesterday_ds_nodash }}` lp
    on cp.product_id = lp.product_id
where cp.master_track_id <> lp.master_track_id
      and cast(cp.master_artist_id as string) not in (select master_artist_id from  `{{ var.value.metadata_dst }}.blacklist_artist`)
      and (datetime_diff(datetime(cp.product_release_date),
                         datetime('{{ ds }}'), DAY) > 28
           or datetime_diff(datetime(cp.project_release_date),
                         datetime('{{ ds }}'), DAY) > 28))

select distinct a.master_track_id
from
(select distinct master_track_id
from tracks
cross join unnest(master_track_id) as master_track_id) a
left outer join (select * from `{{ var.value.metadata_dst }}.changeset_track_weekly`
                 where _partitiontime = timestamp('{{ ti.xcom_pull("set_context")["end_of_week"] }}')) cst
    on cst.master_track_id = a.master_track_id
where cst.master_track_id is null
