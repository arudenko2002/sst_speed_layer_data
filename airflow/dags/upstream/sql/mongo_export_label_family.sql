#standardSQL

select distinct label_family_id as _id,
label_family_name as name,
current_timestamp() as load_datetime
from `{{ var.value.metadata_src }}.product`
