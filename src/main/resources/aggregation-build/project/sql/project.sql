select
${PERIOD_MARKER} as period_marker,
cast(${PERIOD} as string) as period,
${TERRITORY_MARKER} as territory_marker,
cast(${TERRITORY} as string) as territory,
${TERRITORY_SUBDIVISION} as territory_subdivision,
cast(${PARTNER} as string) as partner,
${LABEL_MARKER} as label_marker,
cast(${LABEL} as string) as label,
cast(project_artist_id as string) as master_artist_id,
cast(IF(project_id=-1, (100 * master_artist_id) + 1, project_id) as string) as project_id,
sum(units) as units,
sum(physical_album_units) as physical_album_units,
sum(digital_album_units) as digital_album_units,
sum(digital_track_units) as digital_track_units,
sum(stream_units) as stream_units,
sum(album_adjusted_units) as album_adjusted_units,
sum(stream_album_adjusted_units) as stream_album_adjusted_units,
sum(digital_track_album_adjusted_units) as digital_track_album_adjusted_units,
sum(euro) as euro,
sum(physical_album_euro) as physical_album_euro,
sum(digital_album_euro) as digital_album_euro,
sum(digital_track_euro) as digital_track_euro,
sum(stream_euro) as stream_euro,
sum(digital_album_album_adjusted_units) as digital_album_album_adjusted_units,
sum(physical_album_album_adjusted_units) as physical_album_album_adjusted_units,
${SHARD_NUMBER} as shard_number,
timestamp('${LOAD_DATETIME}', "America/Los_Angeles") as load_datetime
from `$SST_SOURCE_DATASET.$TABLE_NAME_TRANSACTIONS` $ALIAS_TRANSACTION
inner join (select * from `$SST_SOURCE_DATASET.$TABLE_NAME_PRODUCT` where project_id is not null) $ALIAS_PRODUCT on $ALIAS_TRANSACTION.product_id = $ALIAS_PRODUCT.product_id
${ADDITIONAL_JOIN}
where $ALIAS_TRANSACTION._partitiontime >= timestamp('${PARTITION_START_DATE}') and
$ALIAS_TRANSACTION._partitiontime <= timestamp('${PARTITION_END_DATE}')
--${WHERE_PARTITION_CLAUSE}
${CHANGESET_CLAUSE}
${INCREMENTAL_WHERE_CALUSE}
${ADDITIONAL_WHERE_CALUSE}
group by
master_artist_id,
project_id,
territory_marker,
territory,
territory_subdivision,
partner,
label_marker,
label,
period_marker,
period