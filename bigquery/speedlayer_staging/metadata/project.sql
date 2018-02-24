#standardSQL
with project as (select
ANY_VALUE([cast(master_artist_id as string),  master_artist]) as master_artist_array,
project_id,
project_title,
min(project_release_date) as project_release_date,
min(first_project_activity_week) as first_project_activity_week,
min(earliest_project_release_date) as earliest_project_release_date,
array_agg(distinct sub_genre_code) as genres,
array_agg(distinct label_family_id) as label_families
from `umg-marketing.metadata.product`
where master_artist_id <> -1
and project_title <> ''
group by
project_id,
project_title)

select
p.project_id as _id,
p.project_title as title,
p.master_artist_array[offset(0)] as artist_id,
p.master_artist_array[offset(1)] as artist,
p.project_release_date as release_date,
p.earliest_project_release_date as earliest_release_date,
p.first_project_activity_week as first_activity_week,
ifnull(r.rank, 99999999) as rank,
array(select el
          from unnest(p.genres) as el
          where el is not null
            and el <> '') as genres,
array(select el
          from unnest(p.label_families) as el
          where el is not null) as label_families
from project p
left outer join `umg-dev.consumption_speed_layer_aggregation.top_project_v1_15000104` r on cast(p.project_id as string) = r.project_id
order by rank 