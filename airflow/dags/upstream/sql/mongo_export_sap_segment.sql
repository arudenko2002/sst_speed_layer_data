select distinct sap_segment_code as _id, 
sap_segment_name as name,
`order` as `order`
from `{{ var.value.metadata_dst }}.sap_segment` 
order by `order`