<<<<<<< HEAD
select t1.mean_temp, t1.median_temp,
t1.max_temp, t1.min_temp, t2.days_with_null days_with_null_rr,
t3.total_days - t2.days_with_null days_valid_temp,
cast(concat(cast((t1.decade + 10) as varchar),'0101') as date) - cast(concat(cast((t1.decade) as varchar),'0101') as date) - t3.total_days missing_days_temp,
t4.mean_rr, t4.median_rr,
t4.max_rr, t4.min_rr, t5.days_with_null days_with_null_rr,
t6.total_days - t5.days_with_null days_valid_rr,
cast(concat(cast((t4.decade + 10) as varchar),'0101') as date) - cast(concat(cast((t4.decade) as varchar),'0101') as date) - t6.total_days missing_days_rr,
t4.decade,
t1.decade
=======
select 
	t1.decade decade_temp, t1.mean_temp, t1.median, 
	t1.max_temp, t1.min_temp, 
	case
		when (lead(t1.decade) over(order by t1.decade)) is not null then
						case 
							when t2.counted_days_temp is null then cast(concat(cast((lead(t1.decade) over(order by t1.decade)) as varchar), '0101') as date) - cast(concat(cast(t1.decade as varchar), '0101') as date)        
							else cast(concat(cast((lead(t1.decade) over(order by t1.decade)) as varchar), '0101') as date) - cast(concat(cast(t1.decade as varchar), '0101') as date) - t2.counted_days_temp
						end 
		else null
	end days_not_included_temp,
	t3.decade decade_rr, t3.mean_rr, t3.median_rr, 
	t3.max_rr, t3.min_rr, 
	t3.decade decade_rr,
	case
		when (lead(t3.decade) over(order by t3.decade)) is not null then
						case 
							when t4.counted_days_rr is null then cast(concat(cast((lead(t3.decade) over(order by t3.decade)) as varchar), '0101') as date) - cast(concat(cast(t3.decade as varchar), '0101') as date)        
							else cast(concat(cast((lead(t3.decade) over(order by t3.decade)) as varchar), '0101') as date) - cast(concat(cast(t3.decade as varchar), '0101') as date) - t4.counted_days_rr
						end 
		else null
	end days_not_included_rr
>>>>>>> da39d186a16cdc68461a8c3d1493b1026404cb42
from
/* 
t1 is the table for the median, mean, max, min counting only valid values
*/
(select 
	avg(ebt.tg) mean_temp,
	percentile_cont(0.5) within group(order by ebt.tg) median_temp, 
	max(ebt.tg) max_temp, 
	min(ebt.tg) min_temp,
	(div(extract(year from ebt.date), 10)) * 10 decade
from eca_blend_tg ebt
inner join stations s 
on ebt.staid = s.id 
where ebt.q_tg = 0 and s.cn='DE'
group by div(extract(year from ebt.date), 10)) t1
<<<<<<< HEAD
/* 
t2 is the table for where days that have non valid values are counted
t2 is joined with full outer join in case that there decades in t2 not included in t1
*/
full outer join
(select count(*) days_with_null, div(extract(year from dates_with_null.date_with_null), 10) * 10 decade
from
(select  date date_with_null from eca_blend_tg ebt2 
inner join stations s2 
on s2.id = ebt2.staid and cn = 'DE'
where ebt2.tg is null or ebt2.q_tg != 0
group by date) dates_with_null
group by div(extract(year from dates_with_null.date_with_null), 10) * 10) t2
on t1.decade = t2.decade
/*
t3 table counts all the distinct dates that we have measurements. From these days we substract the days with null values
to get the days with only valid values. Still there is a possibility not to exist dates that there are no measurments at all. We can
get that days by substracting the "total_days" from the total days that a decade has.
*/
inner join
(select count(distinct(ebt3.date)) total_days, div(extract(year from ebt3.date), 10) * 10 decade
from eca_blend_tg ebt3
inner join stations s2
on ebt3.staid = s2.id
where s2.cn = 'DE'
group by decade) t3
on t3.decade = t2.decade
/* we follow the same procedure for the different weather variable and we use a full outer join in case of there decades 
 that are not included at the previous weather variable
 */
=======
left join
(select count(distinct(ebt2.date)) counted_days_temp, div(extract(year from ebt2.date), 10) * 10 decade
from eca_blend_tg ebt2
inner join
stations s2 on s2.id = ebt2.staid
where s2.cn = 'DE' and ebt2.q_tg = 0
group by div(extract(year from ebt2.date), 10)) t2
on t1.decade = t2.decade
>>>>>>> da39d186a16cdc68461a8c3d1493b1026404cb42
full outer join
(select 
	avg(ebr.rr) mean_rr,
	percentile_cont(0.5) within group(order by ebr.rr) median_rr, 
	max(ebr.rr) max_rr, 
	min(ebr.rr) min_rr,
	(div(extract(year from ebr.date), 10)) * 10 decade
from eca_blend_rr ebr
inner join stations s 
<<<<<<< HEAD
on ebr.staid = s.id 
where ebr.q_rr = 0 and s.cn='DE'
group by div(extract(year from ebr.date), 10)) t4
on t4.decade = t1.decade
full outer join
(select count(*) days_with_null, div(extract(year from dates_with_null.date_with_null), 10) * 10 decade
from
(select  date date_with_null from eca_blend_rr ebr2 
inner join stations s2 
on s2.id = ebr2.staid and cn = 'DE'
where ebr2.rr is null or ebr2.q_rr != 0
group by date) dates_with_null
group by div(extract(year from dates_with_null.date_with_null), 10) * 10) t5
on t4.decade = t5.decade
inner join 
(select count(distinct(ebr3.date)) total_days, div(extract(year from ebr3.date), 10) * 10 decade
from eca_blend_rr ebr3
inner join stations s2
on ebr3.staid = s2.id
where s2.cn = 'DE'
group by decade) t6
on t6.decade = t5.decade



=======
on ebr.staid = s.id and ebr.q_rr = 0 and s.cn='DE'
group by div(extract(year from ebr.date), 10)) t3
on t1.decade = t3.decade
left join
(select count(distinct(ebr2.date)) counted_days_rr, div(extract(year from ebr2.date), 10) * 10 decade
from eca_blend_rr ebr2
inner join
stations s3 on s3.id = ebr2.staid
where s3.cn = 'DE' and ebr2.q_rr = 0
group by div(extract(year from ebr2.date), 10)) t4
on t3.decade = t4.decade
>>>>>>> da39d186a16cdc68461a8c3d1493b1026404cb42




	

