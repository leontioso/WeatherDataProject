select t1.mean_temp, t1.median, t1.max_temp, t1.min_temp, t2.days_with_null, t5.days_with_nonull, 3652 - t2.days_with_null - t5.days_with_nonull days_not_included, t1.decade
from
(select 
	avg(ebt.tg) mean_temp,
	percentile_cont(0.5) within group(order by ebt.tg) median, 
	max(ebt.tg) max_temp, 
	min(ebt.tg) min_temp,
	(div(extract(year from ebt.date), 10)) * 10 decade
from eca_blend_tg ebt
inner join stations s 
on ebt.staid = s.id and ebt.q_tg = 0 and s.cn='DE'
group by div(extract(year from ebt.date), 10)) t1
left join
(select count(*) days_with_null, div(extract(year from dates_with_null.date_with_null), 10) * 10 decade
from
(select  date date_with_null from eca_blend_tg ebt2 
inner join stations s2 
on s2.id = ebt2.staid and cn = 'DE'
where ebt2.tg is null or ebt2.q_tg != 0
group by date) dates_with_null
group by div(extract(year from dates_with_null.date_with_null), 10) * 10) t2
on t1.decade = t2.decade
left join
(select count(*) days_with_nonull, div(extract(year from t4.date_with_nonull), 10) * 10 as decade
from
(select t3.date_with_nonull from 
(select  date date_with_nonull from eca_blend_tg ebt3 
inner join stations s3 
on s3.id = ebt3.staid and cn = 'DE'
group by date) t3
where t3.date_with_nonull not in
(select  date date_with_null from eca_blend_tg ebt4 
inner join stations s4 
on s4.id = ebt4.staid and cn = 'DE'
where ebt4.tg is null or ebt4.q_tg != 0
group by date)) t4
group by div(extract(year from t4.date_with_nonull), 10) * 10) t5
on t1.decade = t5.decade

select distinct(ebt.date) date_with_null from eca_blend_tg ebt 
inner join sources s 
on s.staid = ebt.staid and s.souid = ebt.souid and substring(s.elei from 1 for 2) = 'TG' and cn = 'DE'
where ebt.tg is null






	

