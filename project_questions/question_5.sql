create extension if not exists cube;
create extension if not exists earthdistance;

select count(distinct(t4.id_other_stations)) stations, count(distinct(t4.cn)) countries, avg(t4.avg_other_stations) avg_other, avg(t4.temp_avg_closest) avg_closest, t4."month", t4."year"
from
(select t2.*, t3.avg_other_stations, t3.id_other_stations, t3.cords_other_station, t3.cn, t2.avg_closest_station temp_avg_closest
from
(select avg(ebr2.rr) avg_closest_station, t1.staid id_closest_station,
Point(t1.cords[0], t1.cords[1]) cords_closest_station,
extract(month from ebr2.date) "month", extract(year from ebr2.date) "year"
from eca_blend_rr ebr2 
inner join
(select 
	Point(s.cords[0], s.cords[1]) cords, 
	s.id staid,
	(Point(30, 30)<@>Point(s.cords[0], s.cords[1])) * 1.609 distance
from stations s 
left join eca_blend_rr ebr
on s.id = ebr.staid
group by s.cords[0], s.cords[1], s.id
order by distance
limit 1) t1
on t1.staid = ebr2.staid
where ebr2.q_rr = 0
group by  t1.staid, t1.cords[0], t1.cords[1], extract(month from ebr2.date), extract(year from ebr2.date)
order by "year", "month") t2
inner join
(select 
	avg(ebr3.rr) avg_other_stations, ebr3.staid id_other_stations, 
	Point(s3.cords[0], s3.cords[1]) cords_other_station, s3.cn,
	extract(month from ebr3.date) "month", extract(year from ebr3.date) "year"
from eca_blend_rr ebr3
inner join stations s3 on s3.id = ebr3.staid
where ebr3.q_rr  = 0
group by ebr3.staid, s3.cords[0], s3.cords[1], s3.cn, extract(month from ebr3.date), extract(year from ebr3.date)
order by "year", "month") t3
on t2."month" = t3."month" and t2."year" = t3."year" and (t2.cords_closest_station<@>t3.cords_other_station) * 1.609 < 200) t4
group by t4."month", t4."year"
order by t4."year", t4."month"