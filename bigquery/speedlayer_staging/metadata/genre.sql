--standard SQL

select distinct sub_genre_code as _id,
sub_genre_name as name
from metadata.product
where sub_genre_code <> ''
