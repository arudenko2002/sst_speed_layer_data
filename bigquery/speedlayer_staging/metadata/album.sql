--standard SQL

select
master_album_id as _id,
master_album as title,
r2_release_version_title as version,
master_artist_array[offset(0)] as artist_id,
master_artist_array[offset(1)] as artist,
project_id,
project_title,
original_release_date as release_date,
genres,
labels,
label_families
from
(select
master_album_id,
master_album,
ANY_VALUE([master_artist_id,  master_artist]) as master_artist_array,
project_id,
project_title,
r2_release_version_title,
min(original_release_date) as original_release_date,
array_agg(distinct sub_genre_code) as genres,
array_agg(distinct sap_financial_label_code) as labels,
array_agg(distinct label_family_id) as label_families
from `umg-marketing.metadata.product`
where master_artist_id <> '-1'
and master_album_id <> ''
and master_album <> ''
group by
master_album_id,
master_album,
r2_release_version_title,
project_id,
project_title) a

