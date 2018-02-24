
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
      from `umg-swift.consumption_speedlayer_staging.transactions`
      where load_datetime <= timestamp('2017-12-24 07:38:16')
      group by partner_report_date
  ) dst
left outer join 
  (select case when partner_report_date <= cast('2014-12-31' as date) then cast('1999-01-01' as date) else partner_report_date end as partner_report_date, 
      sum(units) as units,
      sum(album_adjusted_units) as album_adjusted_units,
      sum(track_adjusted_units) as track_adjusted_units,
      sum(euro_amount) as euro
      from `umg-swift.consumption.combined_transactions`
      where load_datetime <= timestamp('2017-12-24 07:38:16')
            and product_type in ('Physical', 'Bundle', 'Track', 'Video')
            and subject_area in ('Physical Sales', 'Digital Sales', 'Mobile Sales', 'Streams Sales') 
            and usage_group not in ('Merchandise', 'Other', 'Download (Other)')
            and usage_type not in ('Ephemeral Play', 'Fixed Plays')
            and sales_country_code not in ('ZZ')
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
      group by partner_report_date
  ) src
    on src.partner_report_date = dst.partner_report_date
where cast(src.units - dst.units as int64) <> 0
    or cast(src.album_adjusted_units - dst.album_adjusted_units as int64) <> 0
    or cast(src.track_adjusted_units - dst.track_adjusted_units as int64) <> 0
    or cast(src.euro - dst.euro as int64) <> 0
order by src.partner_report_date desc