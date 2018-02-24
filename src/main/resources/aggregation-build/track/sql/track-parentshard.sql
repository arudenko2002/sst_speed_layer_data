select
${PERIOD_MARKER} as period_marker,
cast(${PERIOD} as string) as period,
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
${SHARD_NUMBER} as shard_number,
max($ALIAS_PARENT_SHARD.load_datetime) as load_datetime
from ${PARENT_SHARD_TABLE} $ALIAS_PARENT_SHARD
inner join `$SST_SOURCE_DATASET.$TABLE_NAME_DAY` $ALIAS_DAY on DATE($ALIAS_PARENT_SHARD._PARTITIONTIME) = $ALIAS_DAY.day
AND cast($ALIAS_PARENT_SHARD.period as int64) = $ALIAS_DAY.${PARENT_PERIOD_COLUMN}
 --${WHERE_PARTITION_CLAUSE}
where $ALIAS_PARENT_SHARD._partitiontime >= timestamp('${PARTITION_START_DATE}') and
$ALIAS_PARENT_SHARD._partitiontime <= timestamp('${PARTITION_END_DATE}')
${CHANGESET_CLAUSE}
${INCREMENTAL_WHERE_CALUSE}
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
usage_type_tier