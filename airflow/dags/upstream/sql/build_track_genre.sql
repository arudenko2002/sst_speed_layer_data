select distinct(master_track_id), sub_genre_code, track_adjusted_units, rank_genre from
(
select master_track_id,
sub_genre_code,
sum(track_adjusted_units) track_adjusted_units,
dense_rank() over(partition by
                      master_track_id
                      --genre_code
                      order by sum(track_adjusted_units) desc) as rank_genre
FROM
(
SELECT master_track_id, 
sub_genre_code, 
sum(track_adjusted_units) as track_adjusted_units
FROM 
`{{ var.value.transactions_dst }}` t
inner join `{{ var.value.metadata_dst }}.product_{{ ds_nodash }}` p
on t.product_id = p.product_id
where
master_track_id <> -1
and master_track_id is not null 
--and sub_genre_code <> ''
group by master_track_id,
sub_genre_code
) g
group by master_track_id,
sub_genre_code
) tmp where rank_genre = 1