#stadardSQL

-- WARNING: contains multiple statements, so select first before running and use "Run Selected" option in BQ
select load_datetime, sum(album_adjusted_units) as album_adjusted_units
from `umg-swift.consumption_speedlayer_aggregation.artist_15000101`
where master_artist_id = '10021875'
and _PARTITIONTIME >= "2017-11-24 00:00:00" AND _PARTITIONTIME < "2017-11-25 00:00:00"
group by load_datetime
order by load_datetime

SELECT sum(album_adjusted_units) as album_adjusted_units
FROM `umg-swift.consumption_speedlayer_staging.transactions` t
inner join `umg-swift.consumption_speedlayer_staging.product_20171128` p on t.product_id = p.product_id
WHERE t._PARTITIONTIME >= "2017-11-24 00:00:00" AND t._PARTITIONTIME < "2017-11-25 00:00:00"
and p.master_artist_id = 10021875
--and cast(t.load_datetime as date) between cast('2017-11-25' as date) and cast('2017-11-28' as date)

SELECT * FROM `umg-swift.consumption_speedlayer_bigtable.artist_v1`
where rowkey = '10021875#A#ALL#ALL#A#ALL#D#20171124#'

SELECT  sum(album_adjusted_units) as album_adjusted_units, 
sum(physical_album_units) as physical_album_units, 
sum(digital_album_album_adjusted_units) as digital_album_album_adjusted_units, 
sum(digital_track_album_adjusted_units) as digital_track_album_adjusted_units, 
sum(audio_stream_album_adjusted_units) as audio_stream_album_adjusted_units, 
sum(video_stream_album_adjusted_units) as video_stream_album_adjusted_units, 
sum(stream_album_adjusted_units) as stream_album_adjusted_units
FROM `umg-swift.consumption_speedlayer_staging.transactions` t
inner join `umg-swift.consumption_speedlayer_staging.product_20171128` p on t.product_id = p.product_id
WHERE t._PARTITIONTIME >= "2017-11-24 00:00:00" AND t._PARTITIONTIME < "2017-11-25 00:00:00"
and p.master_artist_id = 10021875
and physical_album_units <> album_adjusted_units 
and physical_album_units <> 0