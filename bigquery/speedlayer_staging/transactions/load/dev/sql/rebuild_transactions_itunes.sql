select t.transaction_date as transaction_date,
t.product_id as product_id,
t.sales_country_code as sales_country_code,
t.dma_number as dma_number,
t.country_subdivision_code as country_subdivision_code,
'7971'  as master_account_code,
case when lower(t.usage_type) like '%free%' then 'Free' else 'Paid' end as free_paid,
ifnull(lp.sap_segment, '') as sap_segment_name,
ifnull(lp.sap_segment_code, '') as sap_segment_code,
sum(ifnull(t.units, cast(0 as float64))) as units,
sum(ifnull(t.album_adjusted_units, cast(0 as float64))) as album_adjusted_units,
sum(ifnull(t.track_adjusted_units, cast(0 as float64))) as track_adjusted_units,
sum(if(t.product_type in ('Album', 'Bundle') and t.subject_area = 'P', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as physical_album_units,
sum(if(t.product_type in ('Album', 'Bundle') and t.subject_area in ('Digital Sales', 'Mobile Sales'), ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as digital_album_units,
sum(if(t.product_type in ('Track', 'Video') and t.subject_area in ('Digital Sales', 'Mobile Sales'), ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as digital_track_units,
sum(if(t.product_type in ('Track', 'Video') and t.subject_area in ('Digital Sales', 'Mobile Sales'), ifnull(t.album_adjusted_units, cast(0 as float64)), cast(0 as float64))) as digital_track_album_adjusted_units,
sum(if(t.product_type in ('Track', 'Video') and t.subject_area = 'Streams Sales', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as stream_units,
sum(if(t.product_type in ('Track', 'Video') and t.subject_area = 'Streams Sales', ifnull(t.album_adjusted_units, cast(0 as float64)), cast(0 as float64))) as stream_album_adjusted_units,
sum(if(t.product_type in ('Track', 'Video') and t.subject_area = 'Streams Sales', ifnull(t.track_adjusted_units, cast(0 as float64)), cast(0 as float64))) as stream_track_adjusted_units,
sum(if(t.product_type = 'Track' and t.subject_area = 'Streams Sales', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as audio_stream_units,
sum(if(t.product_type = 'Track' and t.subject_area = 'Streams Sales', ifnull(t.album_adjusted_units, cast(0 as float64)), cast(0 as float64))) as audio_stream_album_adjusted_units,
sum(if(t.product_type = 'Track' and t.subject_area = 'Streams Sales', ifnull(t.track_adjusted_units, cast(0 as float64)), cast(0 as float64))) as audio_stream_track_adjusted_units,
sum(if(t.product_type = 'Video' and t.subject_area = 'Streams Sales', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as video_stream_units,
sum(if(t.product_type = 'Video' and t.subject_area = 'Streams Sales', ifnull(t.album_adjusted_units, cast(0 as float64)), cast(0 as float64))) as video_stream_album_adjusted_units,
sum(if(t.product_type = 'Video' and t.subject_area = 'Streams Sales', ifnull(t.track_adjusted_units, cast(0 as float64)), cast(0 as float64))) as video_stream_track_adjusted_units,
cast(0 as float64) as airplay_units,
sum(ifnull(t.euro_amount, cast(0 as float64))) as euro,
sum(if(t.product_type in ('Album', 'Bundle') and t.subject_area = 'P', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as physical_album_euro,
sum(if(t.product_type in ('Album', 'Bundle') and t.subject_area in ('Digital Sales', 'Mobile Sales'), ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as digital_album_euro,
sum(if(t.product_type in ('Track', 'Video') and t.subject_area in ('Digital Sales', 'Mobile Sales'), ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as digital_track_euro,
sum(if(t.product_type in ('Track', 'Video') and t.subject_area = 'Streams Sales', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as stream_euro,
sum(if(t.product_type = 'Track' and t.subject_area = 'Streams Sales', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as audio_stream_euro,
sum(if(t.product_type = 'Video' and t.subject_area = 'Streams Sales', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as video_stream_euro,
cast(0 as float64) as airplay_euro
from (select * from `umg-marketing.consumption.transactions_itunesdownloads`
      where _partitiontime = timestamp(@datePartition)) t
left outer join  `umg-marketing.metadata.local_product` lp on t.product_id = lp.product_id
                and t.sales_country_code = lp.sales_country_code
                and lp.sales_country_code in ('US','GB','DK','CA','SE','JP','NO','BE','FR','NL','DE','IE')
                and lp.sap_segment_code <> '-'
group by transaction_date,
product_id,
sales_country_code,
dma_number,
country_subdivision_code,
sap_segment_name,
sap_segment_code,
master_account_code,
free_paid
