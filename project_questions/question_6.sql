/*
I worked with same methodology creating three tables for radius 50 km, 100 km and 500 km and then i joined them together
on the same id and date. The difference with the previous task is that I am looking for the surrounding stations' values 
for every null value in a station. Maximum radius of the surrounding is 500km. So I select the dates and stations with null
vaues and then I used an inner join on the same date and on distance between station less tha 500km. After that I grouped that
by date and the stations that i am interested for counting the average of the stations within 500 km radius and creating cases for 
50 km and 100km radius. After that I grouped that per month for every station. The problem was that I had memory issus in my
AirMacBook (256 gb total memory and the rr table in the database is 24 gb) so I used two samples of data about 15% of the original size.
Execution time was 29m and 31s. Some observations from the result is that at least two averages are very close and in some cases 
the three averages are close enough. We could test giving value to the null values of the station as the resuslt of the average of the three averages
for this particular month.
*/
select 
	t4.staid, count(count_50km) monthly_counts_50km, avg(mean_50km) monthly_mean_50km,
	count(count_100km) monthly_counts_100km, avg(mean_100km) monthly_mean_100km,
	count(count_500km) monthly_counts_500km, avg(mean_500km) monthly_mean_500km,
	extract(month from t4.date) "month", extract(year from t4.date) "year"
from
(select t3.staid, t3.date, t3.count_50km, t3.mean_50km, t4.count_100km, t4.mean_100km, t5.count_500km, t5.mean_500km
from
(select t1.staid, count(t2.staid) count_50km, t1.date, avg(t2.rr) mean_50km from
(select ebr.*, s.* 
from eca_blend_rr ebr
inner join stations s 
on ebr.staid = s.id
where s.cn = 'DE' and rr is null) t1
inner join 
(select ebr2.rr, ebr2.date, ebr2.staid, s2.cords
from eca_blend_rr ebr2
inner join stations s2
on ebr2.staid = s2.id
where q_rr = 0) t2
on t1.date = t2.date and (t1.cords<@>t2.cords) * 1.609 < 50
group by t1.staid, t1.date
order by t1.date, t1.staid) t3
inner join 
(select t1.staid, count(t2.staid) count_100km, t1.date, avg(t2.rr) mean_100km from
(select ebr.*, s.* 
from eca_blend_rr ebr
inner join stations s 
on ebr.staid = s.id
where s.cn = 'DE' and rr is null) t1
inner join 
(select ebr2.rr, ebr2.date, ebr2.staid, s2.cords
from eca_blend_rr ebr2
inner join stations s2
on ebr2.staid = s2.id 
where q_rr = 0) t2
on t1.date = t2.date and (t1.cords<@>t2.cords) * 1.609 < 100
group by t1.staid, t1.date
order by t1.date, t1.staid) t4
on t3.staid = t4.staid and t3.date = t4.date
inner join 
(select t1.staid, count(t2.staid) count_500km, t1.date, avg(t2.rr) mean_500km from
(select ebr.*, s.* 
from eca_blend_rr ebr
inner join stations s 
on ebr.staid = s.id
where s.cn = 'DE' and rr is null) t1
inner join 
(select ebr2.rr, ebr2.date, ebr2.staid, s2.cords
from eca_blend_rr ebr2
inner join stations s2
on ebr2.staid = s2.id 
where q_rr = 0) t2
on t1.date = t2.date and (t1.cords<@>t2.cords) * 1.609 < 500
group by t1.staid, t1.date
order by t1.date, t1.staid) t5
on t3.staid = t5.staid and t3.date = t5.date) t4
group by t4.staid, extract(year from t4.date), extract(month from t4.date) 

--alt way 

select 
t3.staid, avg(t3.mean_50km) monthly_mean_50km, avg(t3.mean_100km) monthly_mean_100km, avg(t3.mean_500km) monthly_mean_500km,
extract(year from t3.date), extract(month from t3.date)
from
(select 
t1.staid, 
t1.date,
sum(case
		when (t1.cords<@>t2.cords) * 1.609 <= 50 then 1
	end) counts_50km,
sum(case
		when (t1.cords<@>t2.cords) * 1.609 <= 50 then t2.rr
	end) / sum(case
		when (t1.cords<@>t2.cords) * 1.609 <= 50 then 1
	end) mean_50km,
sum(case
		when (t1.cords<@>t2.cords) * 1.609 <= 100 then 1
	end) counts_100km,
sum(case
		when (t1.cords<@>t2.cords) * 1.609 <= 100 then t2.rr
	end) / sum(case
		when (t1.cords<@>t2.cords) * 1.609 <= 100 then 1
	end) mean_100km,
count(t2.rr) counts_500km,
avg(t2.rr) mean_500km
from
(select ebr.*, s.* 
from eca_blend_rr ebr tablesample system(15)
inner join stations s
on ebr.staid = s.id
where s.cn = 'DE' and rr is null) t1
inner join 
(select ebr2.rr, ebr2.date, ebr2.staid, s2.cords
from eca_blend_rr ebr2 tablesample system(15)
inner join stations s2
on ebr2.staid = s2.id
where q_rr = 0) t2
on t1.date = t2.date and (t1.cords<@>t2.cords) * 1.609 <= 500
group by t1.staid, t1.date
order by t1.staid, t1.date) t3
group by t3.staid, extract(year from t3.date), extract(month from t3.date)

