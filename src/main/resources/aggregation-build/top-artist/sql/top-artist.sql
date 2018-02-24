select
master_artist_id,
period_marker,
cast(period as string) as period,
cast(lt_period as string)  as lt_period,
territory_marker,
cast(territory as string) as territory,
cast(partner as string) as partner,
--genres,
--label_families,
label_marker,
cast(label as string) as label,
dense_rank() over(partition by
                      period_marker,
                      period,
                      territory_marker,
                      territory,
                      partner,
                      label_marker,
                      label order by sum(album_adjusted_units) desc) as rank,
sum(units) as units,
sum(lptd_units) as lptd_units,
sum(physical_album_units) as physical_album_units,
sum(digital_album_units) as digital_album_units,
sum(digital_track_units) as digital_track_units,
sum(audio_stream_units) as audio_stream_units,
sum(video_stream_units) as video_stream_units,
sum(airplay_units) as airplay_units,
sum(album_adjusted_units) as album_adjusted_units,
sum(lptd_album_adjusted_units) as lptd_album_adjusted_units,
sum(digital_track_album_adjusted_units) as digital_track_album_adjusted_units,
sum(audio_stream_album_adjusted_units) as audio_stream_album_adjusted_units,
sum(video_stream_album_adjusted_units) as video_stream_album_adjusted_units,
sum(euro) as euro,
sum(lptd_euro) as lptd_euro,
sum(physical_album_euro) as physical_album_euro,
sum(digital_album_euro) as digital_album_euro,
sum(digital_track_euro) as digital_track_euro,
sum(video_stream_euro) as video_stream_euro,
sum(audio_stream_euro) as audio_stream_euro,
sum(airplay_euro) as airplay_euro,
sum(digital_album_album_adjusted_units) as digital_album_album_adjusted_units,
sum(physical_album_album_adjusted_units) as physical_album_album_adjusted_units,
${SHARD_NUMBER} as shard_number,
timestamp('${LOAD_DATETIME}', "America/Los_Angeles") as load_datetime
from
(
select
${PERIOD_MARKER} as period_marker,
cast(${PERIOD} as string) as period,
${TERRITORY_MARKER} as territory_marker,
cast(${TERRITORY} as string) as territory,
cast(${PARTNER} as string) as partner,
${LABEL_MARKER} as label_marker,
cast(${LABEL} as string) as label,
cast($ALIAS_PRODUCT.master_artist_id as string) as master_artist_id,
sum(units) as units,
sum(physical_album_units) as physical_album_units,
sum(digital_album_units) as digital_album_units,
sum(digital_track_units) as digital_track_units,
sum(audio_stream_units) as audio_stream_units,
sum(video_stream_units) as video_stream_units,
sum(airplay_units) as airplay_units,
sum(album_adjusted_units) as album_adjusted_units,
sum(digital_track_album_adjusted_units) as digital_track_album_adjusted_units,
sum(audio_stream_album_adjusted_units) as audio_stream_album_adjusted_units,
sum(video_stream_album_adjusted_units) as video_stream_album_adjusted_units,
sum(euro) as euro,
sum(physical_album_euro) as physical_album_euro,
sum(digital_album_euro) as digital_album_euro,
sum(digital_track_euro) as digital_track_euro,
sum(video_stream_euro) as video_stream_euro,
sum(audio_stream_euro) as audio_stream_euro,
sum(airplay_euro) as airplay_euro,
sum(digital_album_album_adjusted_units) as digital_album_album_adjusted_units,
sum(physical_album_album_adjusted_units) as physical_album_album_adjusted_units
from `$SST_SOURCE_DATASET.$TABLE_NAME_TRANSACTIONS` $ALIAS_TRANSACTION
inner join `$SST_SOURCE_DATASET.$TABLE_NAME_PRODUCT` $ALIAS_PRODUCT on $ALIAS_TRANSACTION.product_id = $ALIAS_PRODUCT.product_id
left outer join `$SST_SOURCE_DATASET.$TABLE_NAME_BLACKLIST_ARTIST` $ALIAS_BLACKLIST_ARTIST on cast($ALIAS_PRODUCT.master_artist_id as string) = $ALIAS_BLACKLIST_ARTIST.master_artist_id
${ADDITIONAL_JOIN}
where $ALIAS_TRANSACTION._partitiontime >= timestamp('${PARTITION_START_DATE}') and
$ALIAS_TRANSACTION._partitiontime <= timestamp('${PARTITION_END_DATE}')
--${WHERE_PARTITION_CLAUSE}
AND $ALIAS_BLACKLIST_ARTIST.master_artist_id is null
${ADDITIONAL_WHERE_CALUSE}
group by
master_artist_id,
period_marker,
period,
territory_marker,
territory,
partner,
label_marker,
label
) cur left outer join
(
select
${PERIOD_MARKER} as ltd_period_marker,
cast(${PERIOD} as string) as ltd_period,
${TERRITORY_MARKER} as ltd_territory_marker,
cast(${TERRITORY} as string) as ltd_territory,
cast(${PARTNER} as string) as ltd_partner,
${LABEL_MARKER} as ltd_label_marker,
cast(${LABEL} as string) as ltd_label,
cast(p.master_artist_id as string) as ltd_master_artist_id,
sum(units) as lptd_units,
sum(album_adjusted_units) as lptd_album_adjusted_units,
sum(euro) as lptd_euro
from `$SST_SOURCE_DATASET.$TABLE_NAME_TRANSACTIONS` $ALIAS_TRANSACTION
inner join `$SST_SOURCE_DATASET.$TABLE_NAME_PRODUCT` $ALIAS_PRODUCT on $ALIAS_TRANSACTION.product_id = $ALIAS_PRODUCT.product_id
left outer join `$SST_SOURCE_DATASET.$TABLE_NAME_BLACKLIST_ARTIST` $ALIAS_BLACKLIST_ARTIST on cast($ALIAS_PRODUCT.master_artist_id as string) = $ALIAS_BLACKLIST_ARTIST.master_artist_id
${ADDITIONAL_JOIN}
where $ALIAS_TRANSACTION._partitiontime >= timestamp('${PARTITION_PREV_START_DATE}') and
$ALIAS_TRANSACTION._partitiontime <= timestamp('${PARTITION_PREV_END_DATE}')
--${WHERE_PARTITION_CLAUSE}
AND $ALIAS_BLACKLIST_ARTIST.master_artist_id is null
${ADDITIONAL_WHERE_CALUSE}
group by
ltd_master_artist_id,
ltd_period_marker,
ltd_period,
ltd_territory_marker,
ltd_territory,
ltd_partner,
ltd_label_marker,
ltd_label
) ltd on
cur.period_marker = ltd.ltd_period_marker
--and cast(findPrevious(cast(cur.period as INT64), cur.PERIOD_MARKER) as string) = ltd.ltd_period
and cur.territory_marker = ltd.ltd_territory_marker
and cur.territory = ltd.ltd_territory
and cur.partner = ltd.ltd_partner
and cur.label_marker = ltd.ltd_label_marker
and cur.label = ltd.ltd_label
and cur.master_artist_id = ltd.ltd_master_artist_id,
(select distinct(${PERIOD})  as lt_period from `$SST_SOURCE_DATASET.$TABLE_NAME_DAY` where day >= DATE('${PARTITION_PREV_START_DATE}')  and day <=
DATE('${PARTITION_PREV_END_DATE}'))  as lt_day
group by
master_artist_id,
period_marker,
period,
lt_period,
territory_marker,
territory,
partner,
label_marker,
label