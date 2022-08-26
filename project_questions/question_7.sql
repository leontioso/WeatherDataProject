select 
	avg(ebr.rr) avg_rr,
	max(ebr.rr) max_rr,
	count(ebr.rr) counts,
	count(distinct(s.id)) stations,
	s.cn
from eca_blend_rr ebr
inner join stations s
on s.id = ebr.staid 
where ebr.q_rr = 0
group by s.cn
order by   stations desc, counts desc, avg_rr desc, max_rr desc


