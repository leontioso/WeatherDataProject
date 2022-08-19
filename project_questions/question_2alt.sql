select tgs.*, rrs.*
from
(select 
	percentile_cont(0.9) within group (order by t1.day_temp) value_90per_temp,
	percentile_cont(0.1) within group (order by t1.day_temp) value_10per_temp,
	count(t1.date) counted_days_temp,
	case 
		when 3652 - count(t1.date) < 0 then 0
		else  3652 - count(t1.date)
	end non_counted_days_temp,
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
group by div(extract(year from t1.date), 10) * 10) tgs
full outer join
(select 
	percentile_cont(0.9) within group (order by t1.day_rr) value_90per_rr,
	percentile_cont(0.1) within group (order by t1.day_rr) value_10per_rr,
	count(t1.date) counted_days_rr,
	case 
		when 3652 - count(t1.date) < 0 then 0
		else  3652 - count(t1.date)
	end non_counted_days_rr,
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
group by div(extract(year from t1.date), 10) * 10) rrs
on tgs.decade = rrs.decade


