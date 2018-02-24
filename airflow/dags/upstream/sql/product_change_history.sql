#standardSQL
select distinct cp.product_id as product_id,
lp.master_track_id as old_master_track_id,
cp.master_track_id as new_master_track_id,
lp.master_album_id as old_master_album_id,
cp.master_album_id as new_master_album_id,
lp.project_id as old_project_id,
cp.project_id as new_project_id,
lp.master_artist_id as old_master_artist_id,
cp.master_artist_id as new_master_artist_id,
'{{ ts }}' as load_datetime
from `{{ var.value.metadata_dst }}.product_{{ ds_nodash }}` cp
inner join `{{ var.value.metadata_dst }}.product_{{ yesterday_ds_nodash }}` lp
    on cp.product_id = lp.product_id
left outer join `{{ var.value.metadata_dst }}.blacklist_artist` blma
    on cast(cp.master_artist_id as string) = blma.master_artist_id
where blma.master_artist_id is null
      and (cp.master_track_id <> lp.master_track_id
        or cp.master_album_id <> lp.master_album_id
        or cp.project_id <> lp.project_id
        or cp.master_artist_id <> lp.master_artist_id )
