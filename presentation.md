<h1> A Project to get familiar with SQL programming language</h1>
<h3> Introduction </h3>
<p>The purpose of this project is to get familiar with SQL language using Postgres SQl relational database system. The data tha were used for this project are raw stuctured relational weather data from the European Assement and Dataset project <a> www.ecad.eu </a>. In particular the source of the data is <a> https://www.ecad.eu/dailydata/predefinedseries.php </a> the predifined eca blended data set.</p>
<p>The weather variables that were studied are Daily Mean Temperature (TG) and Daily Precipitation Amount (RR). For this reason <a> https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_tg.zip</a> and <a>https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_rr.zip</a> datasets were downloaded. The downloaded data were folders than include data in txt files. There are three types of information in the folders. The information about the sources (sources.txt), about the elements (elements.txt), the stations (stations.txt) and the files with measurements results across different dates grouped by station id.</p>
<h2>Preparation of the data</h2>
<p>All the data were loaded to a postgres database with the sql command "COPY ... FROM PROGRAM". The command for the "COPY .. FROM PROGRAM" is applying to a bash terminal and I used the tail programme to read files because I didn't want the first n rows from the text file. The command for "COPY ... PROGRAM" is 'tail -q -n +"number_of_lines_to_be_ignored"  /path_to_dowloaded_data/ * / "elements.txt or station.txt or sources.txt". For these specific type of information data were loaded as rows in a temp table and then they were separated in fixed size colummns because commas existed in values. The measurements for weather variable were loaded direct to tables with the command 'tail -q -n + "number_of_lines_to_be_ignored"  /path_to_dowloaded_data/ eca_blend_"weather_variable_code" / STAID*.txt using in the sql command "COPY ....FROM PROGRAM" the option " DELIMITER ',' ".</p>
The database named WeatherData with tables stations, sources, elements, eca_blend_tg and eca_blend_rr. Sources table was cleaned from unecessary data joing the eca tables on weather variable substring, the measurement date between the start and the stop of source, and the staid of eca table being the same with the staid of sources. After that duplicate rows were deleted from sources using row_number() function. At stations table id column added as a primary key to the table for optimised joinings.
<p>At the end eca tables were updated with null for missing values. At text files missing values had value -9999.</p>
<h3> Project Tasks</h3>
<p>For the purpose of the project the project manager made eight tasks that needed solution by manipulating the weatherdata. The process of solving these tasks gives a better understanding of the sql commands, the structure of sql and the possibilities of optimisation.</p>
<ul>
<li>1) Find the mean, the median, the max, the min, the number of days with missing records for temperature and rain for Germany for every decade,</li>
<li>2) 10th percentile, 90th percentile, number of days counted, number of days with missing records, for German for every decade,</li>
<li>3) Maximum number of days with cumulative rain per decade for German, France and Greece</li> 
<li>4) Number of days with temperature higher than the 90th percentile of the decade for all the available countries</li>
<li>5) For a random combination of cordinates find the closest station and count the mean monthly rain of the station and the surroundings stations in a radius of 200 km. Return the number of the stations and the number of countries that came up and the mean monthly rain.</li>
<li>6) In German for every station with missing values, impute the mean rain of the surroundings stations for the same month. Select a radius from 50 km , 100 km, 500 km. Write down your observations</li>
<li>7) Which country is better for studying the mean rain and why?</li>
<li>8) Is there any relation between the monthly mean rain among countries?</li>
</ul>
<h3>Task 1</h3>

```SQl
select t1.mean_temp, t1.median_temp,
t1.max_temp, t1.min_temp, t2.days_with_null days_with_null_rr,
t3.total_days - t2.days_with_null days_valid_temp,
case 
	when t1.decade is not null then cast(concat(cast((t1.decade + 10) as varchar),'0101') as date) - cast(concat(cast((t1.decade) as varchar),'0101') as date) - t3.total_days
	else null
end missing_days_temp,
t4.mean_rr, t4.median_rr,
t4.max_rr, t4.min_rr, t5.days_with_null days_with_null_rr,
t6.total_days - t5.days_with_null days_valid_rr,
case 
	when t4.decade is not null then cast(concat(cast((t4.decade + 10) as varchar),'0101') as date) - cast(concat(cast((t4.decade) as varchar),'0101') as date) - t6.total_days 
	else null
end missing_days_rr,
t4.decade,
t1.decade
from
/* 
t1 is the table for the median, mean, max, min counting only valid values
*/
(select 
	avg(ebt.tg) mean_temp,
	percentile_cont(0.5) within group(order by ebt.tg) median_temp, 
	max(ebt.tg) max_temp, 
	min(ebt.tg) min_temp,
	(div(extract(year from ebt.date), 10)) * 10 decade
from eca_blend_tg ebt
inner join stations s 
on ebt.staid = s.id 
where ebt.q_tg = 0 and s.cn='DE'
group by div(extract(year from ebt.date), 10)) t1
/* 
t2 is the table for where days that have non valid values are counted
t2 is joined with full outer join in case that there decades in t2 not included in t1
*/
full outer join
(select count(*) days_with_null, div(extract(year from dates_with_null.date_with_null), 10) * 10 decade
from
(select  date date_with_null from eca_blend_tg ebt2 
inner join stations s2 
on s2.id = ebt2.staid and cn = 'DE'
where ebt2.tg is null or ebt2.q_tg != 0
group by date) dates_with_null
group by div(extract(year from dates_with_null.date_with_null), 10) * 10) t2
on t1.decade = t2.decade
/*
t3 table counts all the distinct dates that we have measurements. From these days we substract the days with null values
to get the days with only valid values. Still there is a possibility not to exist dates that there are no measurments at all. We can
get that days by substracting the "total_days" from the total days that a decade has.
*/
inner join
(select count(distinct(ebt3.date)) total_days, div(extract(year from ebt3.date), 10) * 10 decade
from eca_blend_tg ebt3
inner join stations s2
on ebt3.staid = s2.id
where s2.cn = 'DE'
group by decade) t3
on t3.decade = t2.decade
/* we follow the same procedure for the different weather variable and we use a full outer join in case of there decades 
 that are not included at the previous weather variable
 */
full outer join
(select 
	avg(ebr.rr) mean_rr,
	percentile_cont(0.5) within group(order by ebr.rr) median_rr, 
	max(ebr.rr) max_rr, 
	min(ebr.rr) min_rr,
	(div(extract(year from ebr.date), 10)) * 10 decade
from eca_blend_rr ebr
inner join stations s 
on ebr.staid = s.id 
where ebr.q_rr = 0 and s.cn='DE'
group by div(extract(year from ebr.date), 10)) t4
on t4.decade = t1.decade
full outer join
(select count(*) days_with_null, div(extract(year from dates_with_null.date_with_null), 10) * 10 decade
from
(select  date date_with_null from eca_blend_rr ebr2 
inner join stations s2 
on s2.id = ebr2.staid and cn = 'DE'
where ebr2.rr is null or ebr2.q_rr != 0
group by date) dates_with_null
group by div(extract(year from dates_with_null.date_with_null), 10) * 10) t5
on t4.decade = t5.decade
inner join 
(select count(distinct(ebr3.date)) total_days, div(extract(year from ebr3.date), 10) * 10 decade
from eca_blend_rr ebr3
inner join stations s2
on ebr3.staid = s2.id
where s2.cn = 'DE'
group by decade) t6
on t6.decade = t5.decade
```
execution time in a AirMacBook with M1 processor 10m and 33 stations


