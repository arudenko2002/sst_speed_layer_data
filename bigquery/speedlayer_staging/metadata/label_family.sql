--standard SQL

-- umg-dev.consumption_speed_layer_staging.label_family
-- was created from a spredsheet created by Jon Lee
select distinct label_family_id as _id,
label_family_desc as name
from `umg-dev.consumption_speed_layer_staging.label_family`
