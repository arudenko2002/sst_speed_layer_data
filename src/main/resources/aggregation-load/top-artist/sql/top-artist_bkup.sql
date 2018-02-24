select cp.master_artist_id as master_artist_id,
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
lp.lp_units as lp_units,
lp.lp_album_adjusted_units as lp_album_adjusted_units,
lp.lp_euro as lp_euro,
rtd.rtd_units,
rtd.rtd_album_adjusted_units as rtd_album_adjusted_units,
rtd.rtd_euro
from ${TBL_VAR} cp
inner join (select master_artist_id,
--genres,
--label_families,
territory_marker,
territory,
partner,
label_marker,
label,
sum(units) rtd_units,
sum(album_adjusted_units) as rtd_album_adjusted_units,
sum(euro) rtd_euro
from ${TBL_VAR}
group by
master_artist_id,
--genres,
--label_families,
territory_marker,
territory,
partner,
label_marker,
label) rtd on
cp.master_artist_id = rtd.master_artist_id and
cp.territory_marker = rtd.territory_marker and
cp.territory = rtd.territory and
cp.partner = rtd.partner and
cp.label_marker = rtd.label_marker and
cp.label = rtd.label
--cp.period_marker = rtd.period_marker and
--rtd.period <= cp.period
--and cp.genres = rtd.genres
left outer join (select master_artist_id,
--genres,
period_marker,
period,
territory_marker,
territory,
partner,
label_marker,
label,
units lp_units,
album_adjusted_units as lp_album_adjusted_units,
euro lp_euro
from ${TBL_VAR}
) lp on
cp.master_artist_id = lp.master_artist_id and
cp.period_marker = lp.period_marker and
cp.lt_period = lp.period and
--cast(findPrevious(cast(cp.period as INT64), cp.PERIOD_MARKER) as string) = lp.period and
--cast((cast(cp.period as int64) - 1) as string) = lp.period and
cp.territory_marker = lp.territory_marker and
cp.territory = lp.territory and
cp.partner = lp.partner and
cp.label_marker = lp.label_marker and
cp.label = lp.label
where cp.load_datetime = timestamp('$LOAD_DATETIME', "America/Los_Angeles")
and cp.rank <= 500