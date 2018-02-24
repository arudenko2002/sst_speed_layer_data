select distinct  dma_number as _id, dma_name as name
from
(select distinct  dma_number, dma_name
from `umg-marketing.consumption.transactions_apple_music`
where dma_number <> ''
union all
select distinct  dma_number, dma_name
from `umg-marketing.consumption.transactions_spotify`
where dma_number <> '')