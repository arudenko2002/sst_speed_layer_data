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
(-1 * units) as units,
(-1 * physical_album_units) as physical_album_units,
(-1 * digital_album_units) as digital_album_units,
(-1 * digital_track_units) as digital_track_units,
(-1 * audio_stream_units) as audio_stream_units,
(-1 * video_stream_units) as video_stream_units,
(-1 * album_adjusted_units) as album_adjusted_units,
(-1 * audio_stream_album_adjusted_units) as audio_stream_album_adjusted_units,
(-1 * video_stream_album_adjusted_units) as video_stream_album_adjusted_units,
(-1 * digital_track_album_adjusted_units) as digital_track_album_adjusted_units,
(-1 * euro) as euro,
(-1 * physical_album_euro) as physical_album_euro,
(-1 * digital_album_euro) as digital_album_euro,
(-1 * digital_track_euro) as digital_track_euro,
(-1 * video_stream_euro) as video_stream_euro,
(-1 * audio_stream_euro) as audio_stream_euro,
(-1 * digital_album_album_adjusted_units) as digital_album_album_adjusted_units,
(-1 * physical_album_album_adjusted_units) as physical_album_album_adjusted_units,
shard_number,
timestamp('${LOAD_DATETIME}', "America/Los_Angeles") as load_datetime
from ${TBL_VAR}