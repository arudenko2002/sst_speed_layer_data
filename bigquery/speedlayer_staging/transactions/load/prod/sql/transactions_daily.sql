#standardSQL
select t.partner_report_date as partner_report_date,
t.product_id as product_id,
scr.sst_country_code as sst_country_code,
t.dma_number as dma_number,
t.country_subdivision_code as country_subdivision_code,
cast(ifnull(ma.master_account_code, 999999) as string)  as master_account_code,
ifnull(fp.free_paid, 'Paid') as free_paid,
case when lp.sap_segment_code is null and scr.sst_country_code in ('US','GB','DK','CA','SE','JP','NO','BE','FR','NL','DE','IE') then 'Other' else lp.sap_segment_name end as sap_segment_name,
case when lp.sap_segment_code is null and scr.sst_country_code in ('US','GB','DK','CA','SE','JP','NO','BE','FR','NL','DE','IE') then 'OTH' else lp.sap_segment_code end as sap_segment_code,
sum(if(t.subject_area != 'Pre-Orders', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as units,
sum(ifnull(t.album_adjusted_units, cast(0 as float64))) as album_adjusted_units,
sum(ifnull(t.track_adjusted_units, cast(0 as float64))) as track_adjusted_units,
sum(if(t.product_type in ('Physical', 'Bundle') and t.subject_area = 'Physical Sales', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as physical_album_units,
sum(if(t.product_type in ('Physical', 'Bundle') and t.subject_area = 'Physical Sales', ifnull(t.album_adjusted_units, cast(0 as float64)), cast(0 as float64))) as physical_album_album_adjusted_units,
sum(if(t.product_type in ('Physical', 'Bundle') and t.subject_area in ('Digital Sales', 'Mobile Sales'), ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as digital_album_units,
sum(if(t.product_type in ('Physical', 'Bundle') and t.subject_area in ('Digital Sales', 'Mobile Sales'), ifnull(t.album_adjusted_units, cast(0 as float64)), cast(0 as float64))) as digital_album_album_adjusted_units,
sum(if(t.subject_area = 'Pre-Orders', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as digital_album_preorder_units,
sum(if(t.product_type in ('Track', 'Video') and t.subject_area in ('Digital Sales', 'Mobile Sales'), ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as digital_track_units,
sum(if(t.product_type in ('Track', 'Video') and t.subject_area in ('Digital Sales', 'Mobile Sales'), ifnull(t.album_adjusted_units, cast(0 as float64)), cast(0 as float64))) as digital_track_album_adjusted_units,
sum(if(t.product_type in ('Track', 'Video') and t.subject_area in ('Digital Sales', 'Mobile Sales'), ifnull(t.track_adjusted_units, cast(0 as float64)), cast(0 as float64))) as digital_track_track_adjusted_units,
sum(if(t.product_type in ('Track', 'Video', 'Bundle') and t.subject_area = 'Streams Sales', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as stream_units,
sum(if(t.product_type in ('Track', 'Video', 'Bundle') and t.subject_area = 'Streams Sales', ifnull(t.album_adjusted_units, cast(0 as float64)), cast(0 as float64))) as stream_album_adjusted_units,
sum(if(t.product_type in ('Track', 'Video', 'Bundle') and t.subject_area = 'Streams Sales', ifnull(t.track_adjusted_units, cast(0 as float64)), cast(0 as float64))) as stream_track_adjusted_units,
sum(if(t.product_type in ('Track', 'Bundle') and t.subject_area = 'Streams Sales', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as audio_stream_units,
sum(if(t.product_type in ('Track', 'Bundle') and t.subject_area = 'Streams Sales', ifnull(t.album_adjusted_units, cast(0 as float64)), cast(0 as float64))) as audio_stream_album_adjusted_units,
sum(if(t.product_type in ('Track', 'Bundle') and t.subject_area = 'Streams Sales', ifnull(t.track_adjusted_units, cast(0 as float64)), cast(0 as float64))) as audio_stream_track_adjusted_units,
sum(if(t.product_type = 'Video' and t.subject_area = 'Streams Sales', ifnull(t.units, cast(0 as float64)), cast(0 as float64))) as video_stream_units,
sum(if(t.product_type = 'Video' and t.subject_area = 'Streams Sales', ifnull(t.album_adjusted_units, cast(0 as float64)), cast(0 as float64))) as video_stream_album_adjusted_units,
sum(if(t.product_type = 'Video' and t.subject_area = 'Streams Sales', ifnull(t.track_adjusted_units, cast(0 as float64)), cast(0 as float64))) as video_stream_track_adjusted_units,
cast(0 as float64) as airplay_units,
sum(ifnull(t.euro_amount, cast(0 as float64))) as euro,
sum(if(t.product_type in ('Physical', 'Bundle') and t.subject_area = 'Physical Sales', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as physical_album_euro,
sum(if(t.product_type in ('Physical', 'Bundle') and t.subject_area in ('Digital Sales', 'Mobile Sales'), ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as digital_album_euro,
sum(if(t.product_type in ('Track', 'Video') and t.subject_area in ('Digital Sales', 'Mobile Sales'), ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as digital_track_euro,
sum(if(t.product_type in ('Track', 'Video', 'Bundle') and t.subject_area = 'Streams Sales', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as stream_euro,
sum(if(t.product_type in ('Track', 'Bundle') and t.subject_area = 'Streams Sales', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as audio_stream_euro,
sum(if(t.product_type = 'Video' and t.subject_area = 'Streams Sales', ifnull(t.euro_amount, cast(0 as float64)), cast(0 as float64))) as video_stream_euro,
cast(0 as float64) as airplay_euro,
max(t.load_datetime) as load_datetime
from (select partner_report_date,
             product_id,
             product_type,
             units,
             album_adjusted_units,
             track_adjusted_units,
             sales_country_code,
             country_subdivision_code,
             dma_number,
             sub_account_code,
             usage_group,
             usage_type,
             subject_area,
             euro_amount,
             load_datetime 
             from `umg-swift.consumption.combined_transactions`
      where product_type in ('Physical', 'Bundle', 'Track', 'Video')
            and subject_area in ('Physical Sales', 'Digital Sales', 'Mobile Sales', 'Streams Sales') 
            and usage_group not in ('Merchandise', 'Other', 'Download (Other)')
            and usage_type not in ('Ephemeral Play', 'Fixed Plays')
            and partner_report_date = cast(@datePartition as date)
            and load_datetime <= timestamp('2017-12-24 07:38:16')
            and sub_account_code not in (609414, -- YouTube sub accounts
                                          609470,
                                          610074,
                                          610530,
                                          610072,
                                          610073,
                                          609469,
                                          609471,
                                          609472,
                                          609417,
                                          610075,
                                          607000,
                                          609416,
                                          610528,
                                          609468,
                                          610529,
                                          610071,
                                          627210,
                                          609415,
                                          613692,
                                          610527
      ) 
      union all 
      select parse_date('%Y%m%d', partner_report_date) as partner_report_date,
             product_id,
             product_type,
             cast(ordered_quantity - cancelled_quantity as float64) as units,
             cast(0 as float64) as album_adjusted_units,
             cast(0 as float64) as track_adjusted_units,
             sales_country_code,	
             country_subdivision_code,
             dma_number,
             sub_account_code,
             usage_group,
             usage_type,
             'Pre-Orders' as subject_area,
             cast(0 as float64) as euro_amount,
             load_datetime
       from `umg-marketing.consumption.transactions_preorders`
       where  ordered_quantity - cancelled_quantity <> 0
              and load_datetime <= timestamp('2017-12-24 07:38:16')
              and parse_date('%Y%m%d', partner_report_date) = cast(@datePartition as date)) t
inner join (select distinct sales_country_code, sst_country_code 
            from `umg-swift.consumption_speedlayer_staging.sst_country_region`) scr
            on t.sales_country_code = scr.sales_country_code
left outer join `umg-swift.metadata.master_account` ma on cast(t.sub_account_code as string) = cast(ma.sub_account_code as string)
                 and ma.sales_country_code = t.sales_country_code
left outer join (select distinct lp.product_id, scr.sst_country_code, lp.sap_segment_code, lp.sap_segment_name
                 from `umg-swift.metadata.local_product` lp
                 inner join (select distinct sales_country_code, sst_country_code 
                             from `umg-swift.consumption_speedlayer_staging.sst_country_region`) scr on lp.sales_country_code = scr.sales_country_code
                             
                 inner join `umg-swift.consumption_speedlayer_staging.sap_segment` saps on lp.sap_segment_code = saps.sap_segment_code
                                                                       and scr.sst_country_code = saps.sst_country_code                                                        
                 where lp.sales_country_code in ('US','GB','DK','CA','SE','JP','NO','BE','FR','NL','DE','IE')
                       and lp.sap_segment_code <> '-') lp on t.product_id = lp.product_id
                                                          and scr.sst_country_code = lp.sst_country_code
left outer join `umg-swift.consumption_speedlayer_staging.free_paid_mapping` fp on t.usage_group = fp.usage_group
  and t.usage_type = fp.usage_type
group by partner_report_date,
product_id,
sst_country_code,
dma_number,
country_subdivision_code,
sap_segment_name,
sap_segment_code,
master_account_code,
free_paid
