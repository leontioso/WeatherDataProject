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
(select count(distinct(ebt2.date)) counted_days_temp, div(extract(year from ebt2.date), 10) * 10 decade
from eca_blend_tg ebt2
inner join
stations s2 on s2.id = ebt2.staid
where s2.cn = 'DE' and ebt2.q_tg = 0
group by div(extract(year from ebt2.date), 10)) t2
on t1.decade = t2.decade
full outer join
(select 
	avg(ebr.rr) mean_rr,
	percentile_cont(0.5) within group(order by ebr.rr) median_rr, 
	max(ebr.rr) max_rr, 
	min(ebr.rr) min_rr,
	(div(extract(year from ebr.date), 10)) * 10 decade
from eca_blend_rr ebr
inner join stations s 
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




	

