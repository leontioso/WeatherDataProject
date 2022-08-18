create temp table stations_temp (id integer, staname varchar(40), cn varchar(2),lat varchar(9), lon varchar(10), hght smallint);

copy stations_temp (id, staname, cn, lat, lon, hght)
from program E'tail -q -n +20 /Users/leontiosorfanos/Desktop/SqlProjects/WeatherData/*/stations.txt'
delimiter ',';

create table stations (id integer, staname varchar(40), cn varchar(2), cords Point, hght smallint);

insert into stations (id, staname, cn, cords, hght) 
select st3.id, st3.staname, st3.cn, Point(st3.clear_lat, st3.clear_lon), hght
from
	(select id, staname, cn, 
	   		 Round(
		   		cast(	
					(cast(split_part(lat, ':', 1) as real) + (cast(split_part(lat, ':', 2) as real) / 60) + (cast(split_part(lat, ':', 3) as real) / 3600))
					as numeric), 6) as clear_lat,
			 Round(
		   		cast(	
					(cast(split_part(lon, ':', 1) as real) + (cast(split_part(lon, ':', 2) as real) / 60) + (cast(split_part(lon, ':', 3) as real) / 3600))
					as numeric), 6) as clear_lon,
	        hght,
	   row_number() over(partition by id order by id) rn
from stations_temp st2) st3
where st3.rn =1;

