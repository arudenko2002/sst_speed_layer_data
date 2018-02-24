with track as (select
master_track_id,
master_track,
ANY_VALUE([cast(master_artist_id as string),  master_artist]) as master_artist_array,
case when project_id = -1 then master_artist_id*100 + 1 else project_id end as project_id,
project_title,
min(original_release_date) as original_release_date,
min(earliest_master_track_release_date) as earliest_master_track_release_date,
array_agg(distinct sub_genre_code) as genres,
array_agg(distinct label_family_id) as label_families
from `umg-marketing.metadata.product`
where master_artist_id <> -1
and master_track_id is not null
and master_track <> ''
group by
master_track_id,
master_track,
project_id,
project_title)

select t.master_track_id as _id,
t.master_track as title,
t.master_artist_array[offset(0)] as artist_id,
t.master_artist_array[offset(1)] as artist,
t.project_id,
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
          and el <> 0) as label_families
from track t
left outer join `umg-dev.consumption_speed_layer_aggregation.top_track_v2_15000104` r on cast(t.master_track_id as string) = r.master_track_id
order by rank 