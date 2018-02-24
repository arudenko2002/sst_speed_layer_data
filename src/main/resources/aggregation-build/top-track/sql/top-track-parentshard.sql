CREATE TEMP FUNCTION findPrevious(period INT64, marker STRING)
  RETURNS INT64
  LANGUAGE js AS """
function findPrev(val) {
 if(val.toString().length == 4)
    return val - 1;
 else if (val.toString().length == 6){
    x = val.toString().substring(0,4);
    z = val.toString().substring(4,6);
    if(z - 1 == 0 && marker == 'W'){
      res = (x - 1) + '52';
      return res;
    }else if(z - 1 == 0 && marker == 'Q'){
      res = (x - 1) + '04';
      return res;
    }else if(z - 1 == 0 && marker == 'M'){
      res = (x - 1) + '12';
      return res;
    }else{
      return (val - 1);
    }

 }
}
return findPrev(period);
""";
select
master_track_id,
period_marker,
period,
cast(findPrevious(cast(period as INT64), period_marker) as string) lt_period,
territory_marker,
territory,
partner,
genres,
catalog,
label_marker,
label,
dense_rank() over(partition by
                      period_marker,
                      period,
                      territory_marker,
                      territory,
                      partner,
                      label_marker,
                      label,
                      genres,
                      catalog
                       order by sum(track_adjusted_units) desc) as rank,
sum(units) as units,
sum(lptd_units) as lptd_units,
sum(digital_track_units) as digital_track_units,
sum(video_stream_units) as video_stream_units,
sum(audio_stream_units) as audio_stream_units,
sum(euro) as euro,
sum(lptd_euro) as lptd_euro,
sum(digital_track_euro) as digital_track_euro,
sum(video_stream_euro) as video_stream_euro,
sum(audio_stream_euro) as audio_stream_euro,
sum(track_adjusted_units) as track_adjusted_units,
sum(lptd_track_adjusted_units) as lptd_track_adjusted_units,
sum(audio_stream_track_adjusted_units) as audio_stream_track_adjusted_units,
sum(video_stream_track_adjusted_units) as video_stream_track_adjusted_units,
sum(digital_track_track_adjusted_units) as digital_track_track_adjusted_units,
${SHARD_NUMBER} as shard_number,
max(load_datetime) as load_datetime
from
(
select
${PERIOD_MARKER} as period_marker,
cast(${PERIOD} as string) as period,
territory_marker,
cast(territory as string) as territory,
cast(partner as string) as partner,
genres,
catalog,
label_marker,
cast(label as string) as label,
master_track_id,
sum(units) as units,
sum(0.0) as lptd_units,
sum(digital_track_units) as digital_track_units,
sum(video_stream_units) as video_stream_units,
sum(audio_stream_units) as audio_stream_units,
sum(euro) as euro,
sum(0.0) as lptd_euro,
sum(digital_track_euro) as digital_track_euro,
sum(video_stream_euro) as video_stream_euro,
sum(audio_stream_euro) as audio_stream_euro,
sum(track_adjusted_units) as track_adjusted_units,
sum(0.0) as lptd_track_adjusted_units,
sum(audio_stream_track_adjusted_units) as audio_stream_track_adjusted_units,
sum(video_stream_track_adjusted_units) as video_stream_track_adjusted_units,
sum(digital_track_track_adjusted_units) as digital_track_track_adjusted_units,
max(pt.load_datetime) as load_datetime
from ${PARENT_SHARD_TABLE} pt
inner join `$SST_SOURCE_DATASET.$TABLE_NAME_DAY` $ALIAS_DAY on DATE(pt._PARTITIONTIME) = $ALIAS_DAY.day
AND cast(pt.period as int64) = $ALIAS_DAY.${PARENT_PERIOD_COLUMN}
--${WHERE_PARTITION_CLAUSE}
where pt._partitiontime >= timestamp('${PARTITION_START_DATE}') and
pt._partitiontime <= timestamp('${PARTITION_END_DATE}')
group by
master_track_id,
genres,
catalog,
period_marker,
period,
territory_marker,
territory,
partner,
label_marker,
label
) temp
group by
master_track_id,
genres,
catalog,
period_marker,
period,
territory_marker,
territory,
partner,
label_marker,
label