select
${PERIOD_MARKER} as period_marker,
cast(${PERIOD} as string) as period,
${TERRITORY_MARKER} as territory_marker,
cast(${TERRITORY} as string) as territory,
${TERRITORY_SUBDIVISION} as territory_subdivision,
cast(${PARTNER} as string) as partner,
${LABEL_MARKER} as label_marker,
cast(${LABEL} as string) as label,
${USAGE_TYPE_TIER} as usage_type_tier,
cast(release_artist_id as string) as master_artist_id,
cast(IF(project_id=-1, (100 * master_artist_id) + 1, project_id) as string) as project_id,
cast(master_track_id as string) as master_track_id,
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
timestamp('${LOAD_DATETIME}', "America/Los_Angeles") as load_datetime
from `$SST_SOURCE_DATASET.$TABLE_NAME_TRANSACTIONS` $ALIAS_TRANSACTION
inner join (select * from `$SST_SOURCE_DATASET.$TABLE_NAME_PRODUCT` where master_track_id is not null) $ALIAS_PRODUCT on $ALIAS_TRANSACTION.product_id = $ALIAS_PRODUCT.product_id
${ADDITIONAL_JOIN}
where $ALIAS_TRANSACTION._partitiontime >= timestamp('${PARTITION_START_DATE}') and
$ALIAS_TRANSACTION._partitiontime <= timestamp('${PARTITION_END_DATE}')
${CHANGESET_CLAUSE}
${INCREMENTAL_WHERE_CALUSE}
--${WHERE_PARTITION_CLAUSE}
${ADDITIONAL_WHERE_CALUSE}
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