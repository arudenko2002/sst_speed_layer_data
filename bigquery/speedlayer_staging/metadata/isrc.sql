--standard SQL

select isrc as _id,
product_title as title,
r2_resource_version_title as version,
master_track_id as track_id,
master_track as track_title,
master_artist_id as artist_id,
master_artist as artist,
project_id,
project_title,
product_release_date as release_date,
earliest_resource_release_date as earliest_release_date,
sub_genre_code as genre,
sap_financial_label_code as label,
label_family_id as label_family
from `umg-marketing.metadata.product`
where master_artist_id <> -1
and isrc <> ''


