
select 
src.partner_report_date, 
src.master_account_code,
cast(ifnull(src.units, 0) - ifnull(dst.units, 0) as int64) as units_diff,
cast(ifnull(src.album_adjusted_units,0) - ifnull(dst.album_adjusted_units, 0) as int64) as album_adjusted_units_diff,
cast(ifnull(src.track_adjusted_units,0) - ifnull(dst.track_adjusted_units, 0) as int64) as track_adjusted_units_diff,
cast(ifnull(src.euro, 0) - ifnull(dst.euro, 0) as int64) as euro_diff
from
  (select cast(_partitiontime as date) as partner_report_date, 
      master_account_code, 
      sum(units) as units,
      sum(album_adjusted_units) as album_adjusted_units,
      sum(track_adjusted_units) as track_adjusted_units,
      sum(euro) as euro
      from `umg-swift.consumption_speedlayer_staging.transactions`
      where load_datetime <= timestamp('2017-11-30 15:35:04')
      group by partner_report_date, master_account_code
  ) dst
left outer join 
  (select case when t.partner_report_date <= cast('2016-11-17' as date) then cast('1999-01-01' as date) else t.partner_report_date end as partner_report_date, 
      ma.master_account_code as master_account_code,
      sum(t.units) as units,
      sum(t.album_adjusted_units) as album_adjusted_units,
      sum(t.track_adjusted_units) as track_adjusted_units,
      sum(t.euro_amount) as euro
      from `umg-swift.consumption.combined_transactions` t
      inner join (select distinct sub_account_code, master_account_code 
                  from `umg-swift.metadata.master_account`) ma on t.sub_account_code = ma.sub_account_code
      where load_datetime <= timestamp('2017-11-30 15:35:04')
            and t.product_type in ('Physical', 'Bundle', 'Track', 'Video')
            and t.subject_area in ('Physical Sales', 'Digital Sales', 'Mobile Sales', 'Streams Sales') 
            and t.usage_group not in ('Merchandise', 'Other', 'Download (Other)')
            and t.usage_type not in ('Ephemeral Play', 'Fixed Plays')
            and t.sub_account_code not in (609414, -- YouTube sub accounts
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
      group by partner_report_date, master_account_code
  ) src
    on src.partner_report_date = dst.partner_report_date and cast(src.master_account_code as string) = dst.master_account_code
where cast(src.units - dst.units as int64) <> 0
    or cast(src.album_adjusted_units - dst.album_adjusted_units as int64) <> 0
    or cast(src.track_adjusted_units - dst.track_adjusted_units as int64) <> 0
    or cast(src.euro - dst.euro as int64) <> 0
order by src.partner_report_date desc