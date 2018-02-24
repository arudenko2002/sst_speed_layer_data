#standardSQL
select distinct sub_genre_code as _id,
sub_genre_name as name, 
current_timestamp() as load_datetime
from `{{ var.value.metadata_src }}.product`
where sub_genre_code <> ''
