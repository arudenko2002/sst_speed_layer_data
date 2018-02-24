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
project_id,
units,
physical_album_units,
digital_album_units,
digital_track_units,
stream_units,
album_adjusted_units,
stream_album_adjusted_units,
digital_track_album_adjusted_units,
euro,
physical_album_euro,
digital_album_euro,
digital_track_euro,
stream_euro,
digital_album_album_adjusted_units,
physical_album_album_adjusted_units,
shard_number
from (
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
project_id,
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
shard_number,
max(load_datetime) as load_datetime
from ${TBL_VAR}
$WHERE_CLAUSE
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
period,
shard_number
)
where
digital_track_units > 0.0
or physical_album_units > 0.0
or digital_album_units > 0.0
or stream_units > 0.0
or digital_track_euro > 0.0
or physical_album_euro > 0.0
or digital_album_euro > 0.0
or stream_euro > 0.0
or stream_album_adjusted_units > 0.0
or digital_track_album_adjusted_units > 0.0
or digital_album_album_adjusted_units > 0.0

