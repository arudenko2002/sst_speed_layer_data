with artist as (select master_artist_id,
master_artist,
array_agg(distinct label_family_id) as label_families
from `{{ var.value.metadata_src }}.product`
group by master_artist_id,
master_artist)


select a.master_artist_id as _id,
    a.master_artist as name,
    ifnull(r.rank, 999999999) as rank,
    case when b.master_artist_id is not null then 1
    else 0 end as blacklisted,
    array(select el
          from unnest(a.label_families) as el
          where el is not null
          and el <> 0) as label_families,
current_timestamp() as load_datetime
from artist a
left outer join `{{ var.value.metadata_dst }}.blacklist_artist` b on cast(a.master_artist_id as string) = b.master_artist_id
left outer join `{{ var.value.artist_rank_src }}` r on cast(a.master_artist_id as string) = r.master_artist_id
where r.period = '{{ macros.ds_format(ds, "%Y-%m-%d" , "%Y" ) }}'
order by rank 