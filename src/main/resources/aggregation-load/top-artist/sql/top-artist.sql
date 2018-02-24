select * from
(
select
cp.master_artist_id as master_artist_id,
cp.period_marker as period_marker,
cp.period as period,
cp.lt_period as lt_period,
cp.territory_marker as territory_marker,
cp.territory as territory,
cp.partner as partner,
cp.label_marker as label_marker,
cp.label as label,
cp.rank as rank,
cp.units as units,
cp.lptd_units as lptd_units,
cp.euro as euro,
cp.lptd_euro as lptd_euro,
cp.physical_album_units as physical_album_units,
cp.digital_album_units as digital_album_units,
cp.digital_track_units as digital_track_units,
cp.audio_stream_units as audio_stream_units,
cp.video_stream_units as video_stream_units,
cp.album_adjusted_units as album_adjusted_units,
cp.lptd_album_adjusted_units as lptd_album_adjusted_units,
cp.digital_track_album_adjusted_units as digital_track_album_adjusted_units,
cp.audio_stream_album_adjusted_units as audio_stream_album_adjusted_units,
cp.video_stream_album_adjusted_units as video_stream_album_adjusted_units,
cp.physical_album_euro as physical_album_euro,
cp.digital_album_euro as digital_album_euro,
cp.digital_track_euro as digital_track_euro,
cp.audio_stream_euro as audio_stream_euro,
cp.video_stream_euro as video_stream_euro,
cp.shard_number as shard_number,
cp.digital_album_album_adjusted_units as digital_album_album_adjusted_units,
cp.physical_album_album_adjusted_units as physical_album_album_adjusted_units,
cp.load_datetime as load_datetime,
lag(units, 1) over window_func as lp_units,
lag(album_adjusted_units, 1) over window_func as lp_album_adjusted_units,
lag(euro, 1)  over window_func as lp_euro,
sum(units) over (window_func ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  as rtd_units,
sum(album_adjusted_units) over (window_func ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rtd_album_adjusted_units,
sum(euro) over (window_func ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as rtd_euro
from ${TBL_VAR} cp
WINDOW window_func as  (partition by master_artist_id,
                      period_marker,
                      territory_marker,
                      territory,
                      partner,
                      label_marker,
                      label order by period ASC  )
)
where rank <= 500
--to filter out 1998 and 1999 partition from loading to BT
and period not like ('199%')
and load_datetime = timestamp('$LOAD_DATETIME', "America/Los_Angeles")