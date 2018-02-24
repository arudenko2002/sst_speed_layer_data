#standardSQL
select distinct cp.product_id,
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
left outer join (select * from `{{ var.value.metadata_dst }}.product_changeset`
                 where _partitiontime = timestamp('{{ ti.xcom_pull("set_context")["end_of_week"] }}')) cst
    on cst.product_id = cp.product_id
        and ifnull(cst.old_master_track_id, -33) = ifnull(lp.master_track_id, -33)
        and ifnull(cst.new_master_track_id, -33) = ifnull(cp.master_track_id, -33)
        and ifnull(cst.old_master_album_id, -33) = ifnull(lp.master_album_id, -33)
        and ifnull(cst.new_master_album_id, -33) = ifnull(cp.master_album_id, -33)
        and ifnull(cst.old_project_id, -33) = ifnull(lp.project_id, -33)
        and ifnull(cst.new_project_id, -33) = ifnull(cp.project_id, -33)
        and ifnull(cst.old_master_artist_id, -33) = ifnull(lp.master_artist_id, -33)
        and ifnull(cst.new_master_artist_id, -33) = ifnull(cp.master_artist_id, -33)
where cast(cp.master_artist_id as string) not in (select master_artist_id from  `{{ var.value.metadata_dst }}.blacklist_artist`)
      and cp.project_id <> lp.project_id
      and cst.product_id is null
      and (datetime_diff(datetime(cp.product_release_date),
                         datetime('{{ ds }}'), DAY) > 28
           or datetime_diff(datetime(cp.project_release_date),
                         datetime('{{ ds }}'), DAY) > 28)

