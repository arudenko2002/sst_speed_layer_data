#standardSQL
with artists as (select [cast(lp.master_artist_id as string), cast(cp.master_artist_id as string)] as master_artist_id
from `{{ var.value.metadata_dst }}.product_{{ ds_nodash }}` cp
inner join `{{ var.value.metadata_dst }}.product_{{ yesterday_ds_nodash }}` lp
    on cp.product_id = lp.product_id
where cp.master_artist_id <> lp.master_artist_id
      and cast(cp.master_artist_id as string) not in (select master_artist_id from  `{{ var.value.metadata_dst }}.blacklist_artist`)
      and (datetime_diff(datetime(cp.product_release_date),
                         datetime('{{ ds }}'), DAY) <= 28
           or datetime_diff(datetime(cp.project_release_date),
                         datetime('{{ ds }}'), DAY) <= 28))

select distinct master_artist_id
from artists
cross join unnest(master_artist_id) as master_artist_id



