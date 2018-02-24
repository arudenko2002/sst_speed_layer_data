#standardSQL
with track as (select
master_track_id,
master_track,
release_artist_id,
release_artist,
project_id,
project_title,
min(product_release_date) as original_release_date,
min(earliest_master_track_release_date) as earliest_master_track_release_date,
array_agg(distinct sub_genre_code) as genres,
array_agg(distinct label_family_id) as label_families
from `{{ var.value.metadata_src }}.product`
where master_track_id is not null
group by
master_track_id,
master_track,
release_artist_id,
release_artist,
project_id,
project_title)

select t.master_track_id as _id,
t.master_track as title,
t.release_artist_id as artist_id,
t.release_artist as artist,
case when t.project_id = -1 then t.release_artist_id * 100 + 1 else t.project_id end as project_id,
t.project_title,
t.original_release_date as release_date,
t.earliest_master_track_release_date,
ifnull(r.rank, 99999999) as rank,
array(select el
          from unnest(t.genres) as el
          where el is not null
            and el <> '') as genres,
array(select el
          from unnest(t.label_families) as el
          where el is not null
          and el <> 0) as label_families,
current_timestamp() as load_datetime
from track t
left outer join (select master_track_id, 
                 max(rank) as rank
                 from `{{ var.value.track_rank_src }}`
                 where period_marker = 'Y'
                 group by master_track_id) r on cast(t.master_track_id as string) = r.master_track_id
order by rank 