select global_week as _id, 
min(day_id) as startDate,
max(day_id) as endDate
from `{{ var.value.metadata_src }}.day`
group by global_week
order by global_week