#standardSQL
with albums as (select [cast(lp.master_album_id as string), cast(cp.master_album_id as string)] as master_album_id
from `{{ var.value.metadata_dst }}.product_{{ ds_nodash }}` cp
inner join `{{ var.value.metadata_dst }}.product_{{ yesterday_ds_nodash }}` lp
    on cp.product_id = lp.product_id
where cp.master_album_id <> lp.master_album_id
      and cast(cp.master_artist_id as string) not in (select master_artist_id from  `{{ var.value.metadata_dst }}.blacklist_artist`)
      and (datetime_diff(datetime(cp.product_release_date),
                         datetime('{{ ds }}'), DAY) > 28
           or datetime_diff(datetime(cp.project_release_date),
                         datetime('{{ ds }}'), DAY) > 28))

select distinct a.master_album_id
from
(select distinct master_album_id
from albums
cross join unnest(master_album_id) as master_album_id) a
left outer join (select * from `{{ var.value.metadata_dst }}.changeset_album_weekly`
                 where _partitiontime = timestamp('{{ ti.xcom_pull("set_context")["end_of_week"] }}')) cst
    on cst.master_album_id = a.master_album_id
where cst.master_album_id is null
