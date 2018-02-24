select distinct(project_id), sub_genre_code, track_adjusted_units, rank_genre from
(
select project_id,
sub_genre_code,
sum(track_adjusted_units) track_adjusted_units,
dense_rank() over(partition by
                      project_id
                      --genre_code
                      order by sum(track_adjusted_units) desc) as rank_genre
FROM
(
SELECT project_id, 
sub_genre_code, 
sum(track_adjusted_units) as track_adjusted_units
FROM 
`consumption_speed_layer_staging.transactions_v3` t
inner join `consumption_speed_layer_staging.product` p
on t.product_id = p.product_id
where project_id is not null 
and project_id <> -1
and master_track_id is not null 
--and sub_genre_code <> ''
group by project_id, sub_genre_code
) g
group by project_id,
sub_genre_code
) tmp where rank_genre = 1