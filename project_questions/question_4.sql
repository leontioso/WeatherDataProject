select 
count(t3.date) total_days,
sum(case 
	when t3.temp_day > t3.percent90 then 1
end) days_above_90per,
t3.decade,
t3.cn
from
(select
t2.temp_day,
t2.cn,
t2.date,
t2.decade,
t1.percent90
from 
(select 
	avg(ebt.tg) temp_day,
	ebt.date,
	div(extract(year from t2.date), 10) * 10 decade,
	s.cn
from eca_blend_tg ebt 
inner join stations s 
on s.id = ebt.staid
where ebt.q_tg = 0
group by ebt.date, s.cn) t2
inner join
(select 
	percentile_cont(0.9) within group (order by ebt.tg) percent90,
	div(extract(year from ebt.date), 10) * 10 decade,
	s.cn
from eca_blend_tg ebt 
inner join stations s 
on s.id = ebt.staid
where ebt.q_tg = 0
group by decade, s.cn) t1
on t1.decade = t2.decade and t1.cn = t2.cn) t3
group by t3.cn, t3.decade