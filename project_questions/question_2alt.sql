select table_tg.*, table_rr.*
from
(select 
	t2.value_90per_tg, t2.value_10per_tg, t3.days_missing_values days_missing_values_tg,
	case 
		when t2.decade is null then 3652
		when t4.total_days_counted is null then cast(concat(cast((t2.decade +10) as varchar), '0101') as date) - cast(concat(cast((t2.decade) as varchar), '0101') as date) - t4.total_days_counted
		else cast(concat(cast((t2.decade +10) as varchar), '0101') as date) - cast(concat(cast((t2.decade) as varchar), '0101') as date) - t4.total_days_counted
	end days_not_included_tg,
	t2.decade decade_tg
	from
/* 
 To find the 90percentile and 10percentile I am going to use the percentile_cont() function as I did to find the median. First we create a table with the average values per day in Germany (t1) and then we are trying
 to find the percentiles as we are grouping per decade(t2). 
 */
(select 
	percentile_cont(0.9) within group (order by t1.day_temp) value_90per_tg,
	percentile_cont(0.1) within group (order by t1.day_temp) value_10per_tg,
	div(extract(year from t1.date), 10) * 10 decade
from
(select 
	avg(ebt.tg) day_temp,
	ebt.date
from eca_blend_tg ebt 
inner join stations s 
on ebt.staid = s.id
where s.cn = 'DE' and ebt.q_tg = 0
group by ebt.date) t1
group by div(extract(year from t1.date), 10) * 10) t2
/*After t1 and t2 we select the days that have missing values per decade (t3) and the we join them with t2 with outer join in case that a decade doesn't exist in t2
 */
full outer join
(select count(distinct(ebt2.date)) days_missing_values, div(extract(year from ebt2.date), 10) * 10 decade
from eca_blend_tg ebt2 
inner join stations s 
on s.id = ebt2.staid
where s.cn = 'DE'
group by  div(extract(year from ebt2.date), 10)) t3
on t2.decade = t3.decade
/*When we finish with the days that have missing values we count all the dates that exist per decade (t4) to discover if there dates in a decade that they are not included*/
inner join
(select count(distinct(ebt3.date)) total_days_counted, div(extract(year from ebt3.date), 10) * 10 decade
from eca_blend_tg ebt3
inner join stations s
on ebt3.staid = s.id
where s.cn = 'DE'
group by div(extract(year from ebt3.date), 10)) t4
on t4.decade = t3.decade) table_tg
full outer join
(select 
	t2.value_90per_rr, t2.value_10per_rr, t3.days_missing_values days_missing_values_rr,
	case 
		when t2.decade is null then 3652
		when t4.total_days_counted is null then cast(concat(cast((t2.decade +10) as varchar), '0101') as date) - cast(concat(cast((t2.decade) as varchar), '0101') as date) - t4.total_days_counted
		else cast(concat(cast((t2.decade +10) as varchar), '0101') as date) - cast(concat(cast((t2.decade) as varchar), '0101') as date) - t4.total_days_counted
	end days_not_included_rr,
	t2.decade decade_rr
	from
(select 
	percentile_cont(0.9) within group (order by t1.day_rr) value_90per_rr,
	percentile_cont(0.1) within group (order by t1.day_rr) value_10per_rr,
	div(extract(year from t1.date), 10) * 10 decade
from
(select 
	avg(ebr.rr) day_rr,
	ebr.date
from eca_blend_rr ebr 
inner join stations s 
on ebr.staid = s.id
where s.cn = 'DE' and ebr.q_rr = 0
group by ebr.date) t1
group by div(extract(year from t1.date), 10) * 10) t2
full outer join
(select count(distinct(ebr2.date)) days_missing_values, div(extract(year from ebr2.date), 10) * 10 decade
from eca_blend_rr ebr2 
inner join stations s 
on s.id = ebr2.staid
where s.cn = 'DE' and ebr2.q_rr != 0
group by  div(extract(year from ebr2.date), 10)) t3
on t2.decade = t3.decade
inner join
(select count(distinct(ebr3.date)) total_days_counted, div(extract(year from ebr3.date), 10) * 10 decade
from eca_blend_rr ebr3
inner join stations s
on ebr3.staid = s.id
where s.cn = 'DE'
group by div(extract(year from ebr3.date), 10)) t4
on t4.decade = t3.decade) table_rr
on table_tg.decade_tg = table_rr.decade_rr


