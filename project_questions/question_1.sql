select t1.mean_temp, t1.median_temp,
t1.max_temp, t1.min_temp, t2.days_with_null days_with_null_rr,
t3.total_days - t2.days_with_null days_valid_temp,
case 
	when t1.decade is not null then cast(concat(cast((t1.decade + 10) as varchar),'0101') as date) - cast(concat(cast((t1.decade) as varchar),'0101') as date) - t3.total_days
	else null
end missing_days_temp,
t4.mean_rr, t4.median_rr,
t4.max_rr, t4.min_rr, t5.days_with_null days_with_null_rr,
t6.total_days - t5.days_with_null days_valid_rr,
case 
	when t4.decade is not null then cast(concat(cast((t4.decade + 10) as varchar),'0101') as date) - cast(concat(cast((t4.decade) as varchar),'0101') as date) - t6.total_days 
	else null
end missing_days_rr,
t4.decade,
t1.decade
from
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
inner join
(select count(distinct(ebt3.date)) total_days, div(extract(year from ebt3.date), 10) * 10 decade
from eca_blend_tg ebt3
inner join stations s2
on ebt3.staid = s2.id
where s2.cn = 'DE'
group by decade) t3
on t3.decade = t2.decade
full outer join
(select 
	avg(ebr.rr) mean_rr,
	percentile_cont(0.5) within group(order by ebr.rr) median_rr, 
	max(ebr.rr) max_rr, 
	min(ebr.rr) min_rr,
	(div(extract(year from ebr.date), 10)) * 10 decade
from eca_blend_rr ebr
inner join stations s 
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