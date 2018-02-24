select
period_marker,
period,
territory_marker,
territory,
territory_subdivision,
partner,
label_marker,
label,
master_artist_id,
units,
physical_album_units,
digital_album_units,
digital_track_units,
audio_stream_units,
video_stream_units,
album_adjusted_units,
audio_stream_album_adjusted_units,
video_stream_album_adjusted_units,
digital_track_album_adjusted_units,
euro,
physical_album_euro,
digital_album_euro,
digital_track_euro,
video_stream_euro,
audio_stream_euro,
digital_album_album_adjusted_units,
physical_album_album_adjusted_units,
shard_number
from
(
select
period_marker,
period,
territory_marker,
territory,
territory_subdivision,
partner,
label_marker,
label,
master_artist_id,
sum(units) as units,
sum(physical_album_units) as physical_album_units,
sum(digital_album_units) as digital_album_units,
sum(digital_track_units) as digital_track_units,
sum(audio_stream_units) as audio_stream_units,
sum(video_stream_units) as video_stream_units,
sum(album_adjusted_units) as album_adjusted_units,
sum(audio_stream_album_adjusted_units) as audio_stream_album_adjusted_units,
sum(video_stream_album_adjusted_units) as video_stream_album_adjusted_units,
sum(digital_track_album_adjusted_units) as digital_track_album_adjusted_units,
sum(euro) as euro,
sum(physical_album_euro) as physical_album_euro,
sum(digital_album_euro) as digital_album_euro,
sum(digital_track_euro) as digital_track_euro,
sum(video_stream_euro) as video_stream_euro,
sum(audio_stream_euro) as audio_stream_euro,
sum(digital_album_album_adjusted_units) as digital_album_album_adjusted_units,
sum(physical_album_album_adjusted_units) as physical_album_album_adjusted_units,
shard_number,
max(load_datetime) as load_datetime
from ${TBL_VAR}
$WHERE_CLAUSE
group by
master_artist_id,
territory_marker,
territory,
territory_subdivision,
partner,
label_marker,
label,
period_marker,
period,
shard_number
)
where
physical_album_units > 0.0
or digital_album_units > 0.0
or digital_track_units > 0.0
or digital_track_euro > 0.0
or audio_stream_units > 0.0
or video_stream_units > 0.0
or audio_stream_album_adjusted_units > 0.0
or video_stream_album_adjusted_units > 0.0
or digital_track_album_adjusted_units > 0.0
or physical_album_euro > 0.0
or digital_album_euro > 0.0
or digital_track_euro > 0.0
or video_stream_euro > 0.0
or audio_stream_euro > 0.0
or digital_album_album_adjusted_units > 0.0
or physical_album_album_adjusted_units > 0.0
