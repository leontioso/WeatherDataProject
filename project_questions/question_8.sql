CREATE EXTENSION IF NOT EXISTS tablefunc;


select avg(t1.monthly_avg), t1.cn
from
(select 
avg(ebr.rr) monthly_avg,
extract(month from ebr.date) "month",
extract(year from ebr.date) "year",
cn
from eca_blend_rr ebr
inner join stations s 
on s.id = ebr.staid 
where ebr.q_rr = 0
group by extract(month from ebr.date), extract(year from ebr.date), cn
order by "year", "month") t1
group by t1.cn
order by avg(t1.monthly_avg) desc