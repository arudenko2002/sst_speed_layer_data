select
period_marker,
period,
territory_marker,
territory,
territory_subdivision,
partner,
label_marker,
label,
usage_type_tier,
master_artist_id,
project_id,
master_track_id,
units,
digital_track_units,
video_stream_units,
audio_stream_units,
euro,
digital_track_euro,
video_stream_euro,
audio_stream_euro,
track_adjusted_units,
audio_stream_track_adjusted_units,
video_stream_track_adjusted_units,
digital_track_track_adjusted_units,
digital_track_album_adjusted_units,
audio_stream_album_adjusted_units,
video_stream_album_adjusted_units,
shard_number
from (select
period_marker,
period,
territory_marker,
territory,
territory_subdivision,
partner,
label_marker,
label,
usage_type_tier,
master_artist_id,
project_id,
master_track_id,
sum(units) as units,
sum(digital_track_units) as digital_track_units,
sum(video_stream_units) as video_stream_units,
sum(audio_stream_units) as audio_stream_units,
sum(euro) as euro,
sum(digital_track_euro) as digital_track_euro,
sum(video_stream_euro) as video_stream_euro,
sum(audio_stream_euro) as audio_stream_euro,
sum(track_adjusted_units) as track_adjusted_units,
sum(audio_stream_track_adjusted_units) as audio_stream_track_adjusted_units,
sum(video_stream_track_adjusted_units) as video_stream_track_adjusted_units,
sum(digital_track_track_adjusted_units) as digital_track_track_adjusted_units,
sum(audio_stream_album_adjusted_units) as audio_stream_album_adjusted_units,
sum(video_stream_album_adjusted_units) as video_stream_album_adjusted_units,
sum(digital_track_album_adjusted_units) as digital_track_album_adjusted_units,
shard_number,
max(load_datetime) as load_datetime
from ${TBL_VAR}
--where
$WHERE_CLAUSE
group by
master_artist_id,
territory_marker,
territory,
partner,
label_marker,
label,
period_marker,
period,
project_id,
master_track_id,
territory_subdivision,
usage_type_tier,
shard_number
)
where
digital_track_units > 0.0
or video_stream_units > 0.0
or audio_stream_units > 0.0
or digital_track_euro > 0.0
or video_stream_euro > 0.0
or audio_stream_euro > 0.0
or audio_stream_track_adjusted_units > 0.0
or video_stream_track_adjusted_units > 0.0
or digital_track_track_adjusted_units > 0.0
or audio_stream_album_adjusted_units > 0.0
or video_stream_album_adjusted_units > 0.0
or digital_track_album_adjusted_units > 0.0
