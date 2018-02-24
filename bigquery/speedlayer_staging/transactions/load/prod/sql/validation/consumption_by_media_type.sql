select 
product_id,
sum(units) as units,
sum(album_adjusted_units) as album_adjusted_units,
sum(track_adjusted_units) as track_adjusted_units,
sum(physical_album_units) as physical_album_units,
sum(physical_album_album_adjusted_units) as physical_album_album_adjusted_units,
sum(digital_album_units) as digital_album_units,
sum(digital_album_album_adjusted_units) as digital_album_album_adjusted_units,
sum(digital_track_units) as digital_track_units,
sum(digital_track_album_adjusted_units) as digital_track_album_adjusted_units,
sum(digital_track_track_adjusted_units) as digital_track_track_adjusted_units,
sum(stream_units) as stream_units,
sum(stream_album_adjusted_units) as stream_album_adjusted_units,
sum(stream_track_adjusted_units) as stream_track_adjusted_units,
sum(audio_stream_units) as audio_stream_units,
sum(audio_stream_album_adjusted_units) as audio_stream_album_adjusted_units,
sum(audio_stream_track_adjusted_units) as audio_stream_track_adjusted_units,
sum(video_stream_units) as video_stream_units,
sum(video_stream_album_adjusted_units) as video_stream_album_adjusted_units,
sum(video_stream_track_adjusted_units) as video_stream_track_adjusted_units,
sum(euro) as euro,
sum(physical_album_euro) as physical_album_euro,
sum(digital_album_euro) as digital_album_euro,
sum(digital_track_euro) as digital_track_euro,
sum(stream_euro) as stream_euro,
sum(audio_stream_euro) as audio_stream_euro,
sum(video_stream_euro) as video_stream_euro,
cast(sum(units) - (sum(physical_album_units) + sum(digital_album_units) + sum(digital_track_units) + sum(stream_units)) as int64) as units_diff,
cast(sum(audio_stream_units) - sum(audio_stream_units) as int64) as audio_video_stream_units_diff,
cast(sum(album_adjusted_units) - (sum(physical_album_album_adjusted_units) + sum(digital_album_album_adjusted_units) + sum(digital_track_album_adjusted_units) + sum(stream_album_adjusted_units)) as int64) as album_adjusted_units_diff, 
cast(sum(stream_album_adjusted_units) - (sum(audio_stream_album_adjusted_units) + sum(video_stream_album_adjusted_units)) as int64) as audio_video_stream_album_adjusted_units_diff,
cast(sum(track_adjusted_units) - ( sum(digital_track_track_adjusted_units) + sum(stream_track_adjusted_units)) as int64) as track_adjusted_units_diff,
cast(sum(stream_track_adjusted_units) - (sum(audio_stream_track_adjusted_units) + sum(video_stream_track_adjusted_units)) as int64) as audio_video_track_adjusted_units_diff,
cast(sum(euro) - (sum(physical_album_euro) + sum(digital_album_euro) + sum(digital_track_euro) + sum(stream_euro)) as int64) as euro_diff,
cast(sum(stream_euro) - (sum(audio_stream_euro) + sum(video_stream_euro)) as int64) as audio_video_stream_euro_diff
from `umg-swift.consumption_speedlayer_staging.transactions`
group by product_id
having units_diff <> 0
  or audio_video_stream_units_diff <> 0
  or album_adjusted_units_diff <> 0
  or audio_video_stream_album_adjusted_units_diff <> 0
  or (track_adjusted_units_diff <> 0 and digital_album_units = 0 and digital_track_units <> 0)
  or audio_video_track_adjusted_units_diff <> 0
  or euro_diff <> 0
  or audio_video_stream_euro_diff <> 0
