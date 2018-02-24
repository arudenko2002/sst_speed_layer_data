
select src.partner_report_date,
cast(ifnull(src.units, 0) - ifnull(dst.units, 0) as int64) as units_diff,
cast(ifnull(src.album_adjusted_units,0) - ifnull(dst.album_adjusted_units, 0) as int64) as album_adjusted_units_diff,
cast(ifnull(src.track_adjusted_units,0) - ifnull(dst.track_adjusted_units, 0) as int64) as track_adjusted_units_diff,
cast(ifnull(src.euro, 0) - ifnull(dst.euro, 0) as int64) as euro_diff
from
  (select cast(_partitiontime as date) as partner_report_date, 
      sum(units) as units,
      sum(album_adjusted_units) as album_adjusted_units,
      sum(track_adjusted_units) as track_adjusted_units,
      sum(euro) as euro
      from `umg-swift.consumption_speedlayer_staging.transactions_uat3`
      where load_datetime <= timestamp('2017-10-30 05:45:20') 
      group by partner_report_date
  ) src
left outer join 
  (select case when partner_report_date <= cast('2017-06-22' as date) then cast('1999-01-01' as date) else partner_report_date end as partner_report_date, 
      sum(units) as units,
      sum(album_adjusted_units) as album_adjusted_units,
      sum(track_adjusted_units) as track_adjusted_units,
      sum(euro_amount) as euro
      from `umg-swift.consumption.combined_transactions`
      where load_datetime <= timestamp('2017-10-30 05:45:20')
            and product_type in ('Physical', 'Bundle', 'Track', 'Video')
            and subject_area in ('Physical Sales', 'Digital Sales', 'Mobile Sales', 'Streams Sales') 
            and usage_group not in ('Merchandise', 'Other', 'Download (Other)')
      group by partner_report_date
  ) dst 
    on src.partner_report_date = dst.partner_report_date
where cast(src.units - dst.units as int64) <> 0
    or cast(src.album_adjusted_units - dst.album_adjusted_units as int64) <> 0
    or cast(src.track_adjusted_units - dst.track_adjusted_units as int64) <> 0
    or cast(src.euro - dst.euro as int64) <> 0
order by src.partner_report_date desc