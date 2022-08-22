select t3.*, t4.count, t4.avg, t5.count, t5.avg
from
(select t1.staid, count(t2.staid), t1.date, avg(t2.rr) from
(select ebr.*, s.* 
from eca_blend_rr ebr tablesample system(5)
inner join stations s 
on ebr.staid = s.id
where s.cn = 'DE' and rr is null) t1
inner join 
(select ebr2.rr, ebr2.date, ebr2.staid, s2.cords
from eca_blend_rr ebr2 tablesample system(5)
inner join stations s2
on ebr2.staid = s2.id
where q_rr = 0) t2
on t1.date = t2.date and (t1.cords<@>t2.cords) * 1.609 < 50
group by t1.staid, t1.date
order by t1.date, t1.staid) t3
inner join 
(select t1.staid, count(t2.staid), t1.date, avg(t2.rr) from
(select ebr.*, s.* 
from eca_blend_rr ebr tablesample system(5)
inner join stations s 
on ebr.staid = s.id
where s.cn = 'DE' and rr is null) t1
inner join 
(select ebr2.rr, ebr2.date, ebr2.staid, s2.cords
from eca_blend_rr ebr2 tablesample system(5)
inner join stations s2
on ebr2.staid = s2.id 
where q_rr = 0) t2
on t1.date = t2.date and (t1.cords<@>t2.cords) * 1.609 < 100
group by t1.staid, t1.date
order by t1.date, t1.staid) t4
on t3.staid = t4.staid and t3.date = t4.date
inner join 
(select t1.staid, count(t2.staid), t1.date, avg(t2.rr) from
(select ebr.*, s.* 
from eca_blend_rr ebr tablesample system(5)
inner join stations s 
on ebr.staid = s.id
where s.cn = 'DE' and rr is null) t1
inner join 
(select ebr2.rr, ebr2.date, ebr2.staid, s2.cords
from eca_blend_rr ebr2 tablesample system(5)
inner join stations s2
on ebr2.staid = s2.id 
where q_rr = 0) t2
on t1.date = t2.date and (t1.cords<@>t2.cords) * 1.609 < 500
group by t1.staid, t1.date
order by t1.date, t1.staid) t5
on t3.staid = t5.staid and t3.date = t5.date

