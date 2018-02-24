#standard SQL
with artist as (select master_artist_id,
master_artist
from `umg-marketing.metadata.product`
group by master_artist_id,
master_artist)

select a.master_artist_id as _id,
    a.master_artist as name,
    ifnull(r.rank, 999999999) as rank,
    case when b.artist_id is not null then 1
    else 0 end as blacklisted
from artist a
left outer join `umg-dev.consumption_speed_layer_staging.blacklist_master_artist` b on cast(a.master_artist_id as string) = b.artist_id
left outer join `umg-dev.consumption_speed_layer_aggregation.top_artist_v5_15000104` r on cast(a.master_artist_id as string) = r.master_artist_id
order by rank 
