#standardSQL
select
upc as _id,
product_title as title,
r2_release_version_title as version,
release_artist_id as artist_id,
release_artist as artist, 
case when project_id = -1 then release_artist_id * 100 + 1 else project_id end as project_id,
project_title,
min(product_release_date) as release_date,
current_timestamp() as load_datetime
from `{{ var.value.metadata_src }}.product`
group by
upc,
product_title,
release_artist_id,
release_artist,
r2_release_version_title,
project_id,
project_title

