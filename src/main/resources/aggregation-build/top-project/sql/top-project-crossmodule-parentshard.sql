select
project_id,
period_marker,
cast(period as string) as period,
cast(lt_period as string)  as lt_period,
territory_marker,
cast(territory as string) as territory,
cast(partner as string) as partner,
genres,
catalog,
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
                      label ,
                      genres,
                      catalog order by sum(album_adjusted_units) desc) as rank,
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
sum(audio_stream_euro) as audio_stream_euro,
sum(video_stream_euro) as video_stream_euro,
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
territory_marker,
territory,
partner,
label_marker,
label,
'ALL' as genres,
'ALL' as catalog,
project_id,
sum(units) as units,
sum(physical_album_units) as physical_album_units,
sum(digital_album_units) as digital_album_units,
sum(digital_track_units) as digital_track_units,
sum(audio_stream_units) as audio_stream_units,
sum(video_stream_units) as video_stream_units,
0.0 as airplay_units,
sum(album_adjusted_units) as album_adjusted_units,
sum(digital_track_album_adjusted_units) as digital_track_album_adjusted_units,
sum(audio_stream_album_adjusted_units) as audio_stream_album_adjusted_units,
sum(video_stream_album_adjusted_units) as video_stream_album_adjusted_units,
sum(euro) as euro,
sum(physical_album_euro) as physical_album_euro,
sum(digital_album_euro) as digital_album_euro,
sum(digital_track_euro) as digital_track_euro,
sum(audio_stream_euro) as audio_stream_euro,
sum(video_stream_euro) as video_stream_euro,
sum(digital_album_album_adjusted_units) as digital_album_album_adjusted_units,
sum(physical_album_album_adjusted_units) as physical_album_album_adjusted_units,
0.0 as airplay_euro
from ${CP_PARENT_SHARD_TABLE} $ALIAS_PARENT_SHARD
 inner join `$SST_SOURCE_DATASET.$TABLE_NAME_DAY` $ALIAS_DAY on DATE($ALIAS_PARENT_SHARD._PARTITIONTIME) = $ALIAS_DAY.day AND
 cast($ALIAS_PARENT_SHARD.period as int64) = $ALIAS_DAY.${CP_PARENT_PERIOD_COLUMN}
where $ALIAS_PARENT_SHARD._partitiontime >= timestamp('${PARTITION_START_DATE}') and
$ALIAS_PARENT_SHARD._partitiontime <= timestamp('${PARTITION_END_DATE}')
group by
project_id,
genres,
catalog,
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
${PERIOD_MARKER} as lptd_period_marker,
cast(${PERIOD} as string) as lptd_period,
territory_marker as lptd_territory_marker,
territory as lptd_territory,
partner as lptd_partner,
label_marker as lptd_label_marker,
label as lptd_label,
'ALL' as lptd_genres,
'ALL' as lptd_catalog,
project_id as lptd_project_id,
sum(units) as lptd_units,
sum(album_adjusted_units) as lptd_album_adjusted_units,
sum(euro) as lptd_euro
from ${LPTD_PARENT_SHARD_TABLE} $ALIAS_PARENT_SHARD
 inner join `$SST_SOURCE_DATASET.$TABLE_NAME_DAY` $ALIAS_DAY on DATE($ALIAS_PARENT_SHARD._PARTITIONTIME) = $ALIAS_DAY.day
 AND cast($ALIAS_PARENT_SHARD.period as int64) = $ALIAS_DAY.${LPTD_PARENT_PERIOD_COLUMN}
where $ALIAS_PARENT_SHARD._partitiontime >= timestamp('${PARTITION_PREV_START_DATE}') and
$ALIAS_PARENT_SHARD._partitiontime <= timestamp('${PARTITION_PREV_END_DATE}')
group by
lptd_project_id,
lptd_genres,
lptd_catalog,
lptd_period_marker,
lptd_period,
lptd_territory_marker,
lptd_territory,
lptd_partner,
lptd_label_marker,
lptd_label
) lptd on
cur.period_marker = lptd.lptd_period_marker
--and cast(findPrevious(cast(cur.period as INT64), cur.PERIOD_MARKER) as string) = lptd.lptd_period
and cur.territory_marker = lptd.lptd_territory_marker
and cur.territory = lptd.lptd_territory
and cur.partner = lptd.lptd_partner
and cur.label_marker = lptd.lptd_label_marker
and cur.label = lptd.lptd_label
and cur.project_id = lptd.lptd_project_id
and cur.genres = lptd.lptd_genres
and cur.catalog = lptd.lptd_catalog,
(select distinct(${PERIOD})  as lt_period from `$SST_SOURCE_DATASET.$TABLE_NAME_DAY` where day >= DATE('${PARTITION_PREV_START_DATE}')  and day <=
DATE('${PARTITION_PREV_END_DATE}'))  as lt_day
group by
project_id,
genres,
catalog,
period_marker,
period,
lt_period,
territory_marker,
territory,
partner,
label_marker,
label