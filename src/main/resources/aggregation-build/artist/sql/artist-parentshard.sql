select
${PERIOD_MARKER} as period_marker,
cast(${PERIOD} as string) as period,
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
${SHARD_NUMBER} as shard_number,
max($ALIAS_PARENT_SHARD.load_datetime) as load_datetime
from ${PARENT_SHARD_TABLE} $ALIAS_PARENT_SHARD
inner join `$SST_SOURCE_DATASET.$TABLE_NAME_DAY` $ALIAS_DAY on  DATE($ALIAS_PARENT_SHARD._PARTITIONTIME) = $ALIAS_DAY.day
and cast($ALIAS_PARENT_SHARD.period as int64) = $ALIAS_DAY.${PARENT_PERIOD_COLUMN}
--${WHERE_PARTITION_CLAUSE}
where $ALIAS_PARENT_SHARD._partitiontime >= timestamp('${PARTITION_START_DATE}') and
$ALIAS_PARENT_SHARD._partitiontime <= timestamp('${PARTITION_END_DATE}')
${CHANGESET_CLAUSE}
${INCREMENTAL_WHERE_CALUSE}
group by
master_artist_id,
territory_marker,
territory,
territory_subdivision,
partner,
label_marker,
label,
period_marker,
period