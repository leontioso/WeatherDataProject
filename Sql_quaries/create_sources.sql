create temp table sources_temp (source_row char(173));

create temp table sources_temp2 (staid integer, souid integer, souname varchar(40), cn varchar(2), cords Point,
						   hght smallint, elei varchar(4), start date, stop date, parid smallint, parnam varchar(51));

copy sources_temp (source_row)
from program 'tail -q -n +26 /Users/leontiosorfanos/Desktop/SqlProjects/WeatherData/*/sources.txt';

insert into sources_temp2 (staid, souid, souname, cn, cords, hght, elei, start, stop, parid, parnam)
select cast(substring(source_row from 1 for 5) as integer),
	   cast(substring(source_row from 7 for 6) as integer),
	   trim(substring(source_row from 14 for 40)),
	   trim(substring(source_row from 55 for 2)),
	   Point(
		   Round(
			    cast(
					(cast(split_part(substring(source_row from 58 for 9), ':', 1) as real) +
			 		(cast(split_part(substring(source_row from 58 for 9), ':', 2) as real) / 60) +
					(cast(split_part(substring(source_row from 58 for 9), ':', 3) as real) / 3600)) as numeric), 6),
		   Round(
			    cast(
					(cast(split_part(substring(source_row from 68 for 10), ':', 1) as real) +
			 		(cast(split_part(substring(source_row from 68 for 10), ':', 2) as real) / 60) +
					(cast(split_part(substring(source_row from 68 for 10), ':', 3) as real) / 3600)) as numeric), 6))
			,
	  cast(substring(source_row from 79 for 4) as smallint),
	  trim(substring(source_row from 84 for 4)),
	  cast(substring(source_row from 89 for 8) as date),
	  cast(substring(source_row from 98 for 8) as date),
	  case 
	  	when trim(substring(source_row from 107 for 5)) = '-' then null
		else cast(substring(source_row from 107 for 5) as SMALLINT)
	  end,
	  trim(substring(source_row from 113 for 51))
from sources_temp

create table sources (staid integer, souid integer, souname varchar(40), cn varchar(2), cords Point,
						   hght smallint, elei varchar(4), start date, stop date, parid smallint, parnam varchar(51));
						   
insert into sources (staid, souid, souname, cn, cords, hght, elei, start, stop, parid, parnam)
select st.staid, st.souid, st.souname, st.cn, Point(st.cords[0], st.cords[1]), st.hght, st.elei, st.start, st.stop, st.parid, st.parnam
from sources_temp2 st
inner join eca_blend_rr ebr
on  ebr.staid = st.staid and ebr.souid = st.souid and ebr.date between st.start and st.stop
where substring(st.elei from 1 for 2) = 'RR'

insert into sources (staid, souid, souname, cn, cords, hght, elei, start, stop, parid, parnam)
select st.staid, st.souid, st.souname, st.cn, Point(st.cords[0], st.cords[1]), st.hght, st.elei, st.start, st.stop, st.parid, st.parnam
from sources_temp2 st
inner join eca_blend_tg ebtg
on  ebtg.staid = st.staid and ebtg.souid = st.souid and ebtg.date between st.start and st.stop
where substring(st.elei from 1 for 2) = 'TG'



		   

	  
	  
	  
	  