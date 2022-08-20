select 
	percentile_cont(0.9) within group (order by ebt.tg),
	div(extract(year from ebt.date), 10) * 10 decade,
	s.cn
from eca_blend_tg ebt 
inner join stations s 
on s.id = ebt.staid
where ebt.q_tg = 0
group by decade, s.cn