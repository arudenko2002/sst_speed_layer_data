#standardSQL
with project as (select
project_artist_id,
project_artist,
array_agg(distinct master_artist_id) as master_artists,
case when project_id = -1 then project_artist_id * 100 + 1 else project_id end as project_id,
project_title,
min(project_release_date) as project_release_date,
min(first_project_activity_week) as first_project_activity_week,
min(earliest_project_release_date) as earliest_project_release_date,
array_agg(distinct sub_genre_code) as genres,
array_agg(distinct label_family_id) as label_families
from `{{ var.value.metadata_src }}.product`
group by
project_artist_id,
project_artist,
project_id,
project_title
)

select
p.project_id as _id,
p.project_title as title,
p.project_artist_id as artist_id,
p.project_artist as artist,
array(select el
          from unnest(p.master_artists) as el
          where el is not null) as master_artists,
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
          where el is not null
          and el <> 0) as label_families,
current_timestamp() as load_datetime
from project p
left outer join (select project_id, 
                 max(rank) as rank
                 from `{{ var.value.project_rank_src }}` 
                 where period_marker = 'Y'
                 group by project_id) r on cast(p.project_id as string) = r.project_id
order by rank 