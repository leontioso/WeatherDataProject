<h1> A Project to get familiar with SQL programming language</h1>
<h3> Introduction </h3>
<p>The purpose of this project is to get familiar with SQL language. The data tha I used for this project are raw stuctured relational weather data from the European Assement and Dataset project <a> www.ecad.eu </a>. In particular the source of the data is <a> https://www.ecad.eu/dailydata/predefinedseries.php </a> the predifined eca blended data set.</p>
<p>The weather variables that were studied are Daily Mean Temperature (TG) and Daily Precipitation Amount (RR). For this reason <a> https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_tg.zip</a> and <a>https://knmi-ecad-assets-prd.s3.amazonaws.com/download/ECA_blend_rr.zip</a> datasets were downloaded. The downloaded data were folders than include data in txt files. There are three types of information in the folders. The information about the sources (sources.txt), about the elements (elements.txt), the stations (stations.txt) and the files with measurements results across different dates grouped by station id. The database system that I used for this project is Postgresql</p>
<h2>Preparation of the data</h2>
<p>All data were loaded to a postgres database with the sql command "COPY ... FROM PROGRAM". The command for the "COPY .. FROM PROGRAM" is applied to a bash terminal and I used the tail programme to read files because I didn't want the first n rows from the text file. The command for "COPY ... PROGRAM" is 'tail -q -n +"number_of_lines_to_be_ignored"  /path_to_dowloaded_data/ * / "elements.txt or station.txt or sources.txt". For these specific type of information, data were loaded as rows in a temp table and then they were separated in fixed size colummns because commas existed in values. The records for weather variable were loaded direct to tables with the command 'tail -q -n + "number_of_lines_to_be_ignored"  /path_to_dowloaded_data/ eca_blend_"weather_variable_code" / STAID*.txt using in the sql command "COPY ....FROM PROGRAM" the option " DELIMITER ',' ". I also loaded data in a linux mint operating system where I had issues with the permissions for postgres so I followed another way making the command to be called as "myself" from postgres as user. Another issue between linux and macos was that in macos there was a contraint about the size of the output of the command. So I couldn't combine all the files together to one output, but I loaded the data per file. </p>
I named the database WeatherData with tables stations, sources, elements, eca_blend_tg and eca_blend_rr. Sources table was cleaned from unecessary data joining the eca tables on weather variable substring, the date of the record to be between the start and the stop of source and the staid of eca table being the same with the staid of sources. After that duplicated rows were deleted from sources using row_number() function. At stations table, id column altered to a primary key for optimised joinings.
For the eca blended tables I loaded data into them with a jupyter notebook to execute the load query for every weather variable. For lat an lon I created a column as Point data type and I converted the values of lat and lon that was in degrees units to decimal units of a Point. Point data type is suitable when you want to include geo-functions to your calculations. The rest data types that I used were varchar, integer, smallint and date.
<p>At the end eca tables were updated with null for missing values. At text files missing values had value -9999.</p>

```python
import psycopg2

def create_populate_eca_table(weather_var_code):
    conn = psycopg2.connect('dbname=weatherdata user=postgres password=postgres host=localhost')
    with conn:
        with conn.cursor() as cur:
            cur.execute(f'''create table eca_blend_{weather_var_code} (staid integer, souid integer, date date, {weather_var_code} integer, q_{weather_var_code} integer check( q_{weather_var_code} in (0, 1, 9)));                
                copy eca_blend_{weather_var_code} (staid, souid, date, {weather_var_code}, q_{weather_var_code})
                from program 'sudo runuser -l postgres  -c "sudo tail -q -n +22 /home/leontioso/Desktop/WeatherData/ECA_blend_{weather_var_code}/{weather_var_code.upper()}_STAID* " '
                 delimiter ','
                 ''')
    conn.close()
    print(f'eca_blend_{weather_var_code} table created')
                
def create_populate_sources(code_set):
    conn = psycopg2.connect('dbname=weatherdata user=postgres password=postgres host=localhost')
    with conn:
        with conn.cursor() as cur:
            cur.execute('''create temp table sources_temp (staid integer, souid integer, souname varchar(40), cn varchar(2), lat varchar(9), lon varchar(10),
						   hght smallint, elei varchar(4), start date, stop date, parid varchar(5), parnam varchar(51));

                           create temp table sources_temp2 (staid integer, souid integer, souname varchar(40), cn varchar(2), cords Point,
						   hght smallint, elei varchar(4), start date, stop date, parid smallint, parnam varchar(51));

                           copy sources_temp (staid, souid, souname, cn, lat, lon, hght, elei, start, stop, parid, parnam)
                           from program 'sudo runuser -l postgres  -c "sudo tail -q -n +26 /home/leontioso/Desktop/WeatherData/*/sources.txt " '
                           delimiter ',';
                           
                           
                           
                                                     
                           insert into sources_temp2 (staid, souid, souname, cn, cords, hght, elei, start, stop, parid, parnam)
                           select 
                                staid,
                                souid,
                           	   trim(souname),
                           	   trim(cn),
                           	   Point(
                           		   Round(
                           			    cast(
                           					(cast(split_part(lat, ':', 1) as real) +
                           			 		(cast(split_part(lat, ':', 2) as real) / 60) +
                           					(cast(split_part(lat, ':', 3) as real) / 3600)) as numeric), 6),
                           		   Round(
                           			    cast(
                           					(cast(split_part(lon, ':', 1) as real) +
                           			 		(cast(split_part(lon, ':', 2) as real) / 60) +
                           					(cast(split_part(lon, ':', 3) as real) / 3600)) as numeric), 6))
                           			,
                           	  hght,
                           	  trim(elei),
                           	  start,
                           	 start,
                           	  case 
                           	  	when trim(parid) = '-' then null
                           		else cast(parid as SMALLINT)
                           	  end,
                           	  trim(parnam)
                           from sources_temp;
                           
                           create table sources (staid integer, souid integer, souname varchar(40), cn varchar(2), cords Point,
						   hght smallint, elei varchar(4), start date, stop date, parid smallint, parnam varchar(51));''')
            
            for weather_var_code in code_set:
                cur.execute(f'''
                            insert into sources (staid, souid, souname, cn, cords, hght, elei, start, stop, parid, parnam)
                            select 
                            st2.staid, st2.souid, st2.souname, st2.cn, Point(st2.cords[0], st2.cords[1]), st2.hght, st2.elei, st2.start, st2.stop, st2.parid, st2.parnam
                            from
                            (select st.staid, st.souid, st.souname, st.cn, Point(st.cords[0], st.cords[1]) cords, st.hght, st.elei, st.start, st.stop, st.parid, st.parnam,
                            row_number() over(partition by st.staid, st.souid, st.elei order by st.staid, st.souid, st.elei) rn
                            from sources_temp2 st
                            inner join eca_blend_{weather_var_code} eca_blend
                            on  st.staid = eca_blend.staid  and st.souid = eca_blend.souid  and eca_blend.date between st.start and st.stop
                            where substring(st.elei from 1 for 2) = '{weather_var_code.upper()}') st2
                            where st2.rn = 1
                            ''')
                print(f'Inserted values into table sources using eca_blend_{weather_var_code} table')
    conn.close()
    print('Sources table created and populated')
    
    
def create_populate_elements():
    conn = psycopg2.connect('dbname=weatherdata user=postgres password=postgres host=localhost')
    with conn:
        with conn.cursor() as cur:
            cur.execute('''
                        create temp table elements_temp (element_row varchar(172)) ;
                        create table elements (id varchar(5), "desc" varchar(150), unit varchar(15));

                        copy elements_temp (element_row)
                        from program 'sudo runuser -l postgres  -c "sudo tail -q -n +17 /home/leontioso/Desktop/WeatherData/*/elements.txt " ';

                        insert into elements (id, "desc", unit)
                        SELECT
                        	trim(SUBSTRING(element_row from 1 for 5)),
                        	trim(SUBSTRING(element_row from 7 for 150)),
                        	trim(SUBSTRING(element_row from 158 for 15))
                        FROM elements_temp;

                        alter table elements
                        add primary key (id);
                        ''')
    conn.close()
    print('Table elements created and populated')
            
            
def create_populate_stations():
    conn = psycopg2.connect('dbname=weatherdata user=postgres password=postgres host=localhost')
    with conn:
        with conn.cursor() as cur:
            cur.execute(''' 
                        create temp table stations_temp (id integer, staname varchar(40), cn varchar(2),lat varchar(9), lon varchar(10), hght smallint);

                        copy stations_temp (id, staname, cn, lat, lon, hght)
                        from program 'sudo runuser -l postgres  -c "sudo tail -q -n +20 /home/leontioso/Desktop/WeatherData/*/stations.txt " '
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
                        ''')
    conn.close()
    print('Table stations created and populated')
    
def update_eca_tables_withnull(code_set):
    conn = psycopg2.connect('dbname=weatherdata user=postgres password=postgres host=localhost')
    with conn:
        with conn.cursor() as cur:
            for code in code_set:
                cur.execute(f'''update eca_blend_{code}
                                    set {code} = null
                                    where {code} = -9999
                                ''')
                print(f'Eca_table_{code} updated')
``` 
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
At first task I created a subquery (t1) which is the table for the median, mean, max, min counting only valid values. Then t2 is the table to count how many days days have non valid values per decade. T2 is joined using full outer join with t1 in case that there decades in t2 not included in t1. T3 table includes all the distinct dates that we have records per decade. From these days we substract the days with null values to get the days with only valid values. Still there is a possibility to non existed dates that are not part of our dataset. We can get those days by substracting the "total_days" from the total days that the decade has. We follow the same procedure for the different weather variable and we use a full outer join in case of there decades that are not included at the previous weather variable.

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
inner join
(select count(distinct(ebt3.date)) total_days, div(extract(year from ebt3.date), 10) * 10 decade
from eca_blend_tg ebt3
inner join stations s2
on ebt3.staid = s2.id
where s2.cn = 'DE'
group by decade) t3
on t3.decade = t2.decade
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
execution time in a AirMacBook with M1 processor 10 mins and 33 secs

<h2>Task 2</h2>

 To find the 90percentile and 10percentile I am going to use the percentile_cont() function as I did to find the median. First we create a table with the average values per day in Germany (t1) and then we are trying to find the percentiles as we are grouping per decade(t2). After t1 and t2 we select the days that have missing values per decade (t3) and the we join them with t2 with outer join in case that a decade doesn't exist in t2. When we finish with the days that have missing values, we count all the dates that exist per decade (t4) to discover if there are dates in a decade that are not part of our dataset.

```SQL
select table_tg.*, table_rr.*
from
(select 
	t2.value_90per_tg, t2.value_10per_tg, t3.days_missing_values days_missing_values_tg,
	case 
		when t2.decade is null then 3652
		when t4.total_days_counted is null then cast(concat(cast((t2.decade +10) as varchar), '0101') as date) - cast(concat(cast((t2.decade) as varchar), '0101') as date) - t4.total_days_counted
		else cast(concat(cast((t2.decade +10) as varchar), '0101') as date) - cast(concat(cast((t2.decade) as varchar), '0101') as date) - t4.total_days_counted
	end days_not_included_tg,
	t2.decade decade_tg
	from
(select 
	percentile_cont(0.9) within group (order by t1.day_temp) value_90per_tg,
	percentile_cont(0.1) within group (order by t1.day_temp) value_10per_tg,
	div(extract(year from t1.date), 10) * 10 decade
from
(select 
	avg(ebt.tg) day_temp,
	ebt.date
from eca_blend_tg ebt 
inner join stations s 
on ebt.staid = s.id
where s.cn = 'DE' and ebt.q_tg = 0
group by ebt.date) t1
group by div(extract(year from t1.date), 10) * 10) t2
full outer join
(select count(distinct(ebt2.date)) days_missing_values, div(extract(year from ebt2.date), 10) * 10 decade
from eca_blend_tg ebt2 
inner join stations s 
on s.id = ebt2.staid
where s.cn = 'DE'
group by  div(extract(year from ebt2.date), 10)) t3
on t2.decade = t3.decade
inner join
(select count(distinct(ebt3.date)) total_days_counted, div(extract(year from ebt3.date), 10) * 10 decade
from eca_blend_tg ebt3
inner join stations s
on ebt3.staid = s.id
where s.cn = 'DE'
group by div(extract(year from ebt3.date), 10)) t4
on t4.decade = t3.decade) table_tg
full outer join
(select 
	t2.value_90per_rr, t2.value_10per_rr, t3.days_missing_values days_missing_values_rr,
	case 
		when t2.decade is null then 3652
		when t4.total_days_counted is null then cast(concat(cast((t2.decade +10) as varchar), '0101') as date) - cast(concat(cast((t2.decade) as varchar), '0101') as date) - t4.total_days_counted
		else cast(concat(cast((t2.decade +10) as varchar), '0101') as date) - cast(concat(cast((t2.decade) as varchar), '0101') as date) - t4.total_days_counted
	end days_not_included_rr,
	t2.decade decade_rr
	from
(select 
	percentile_cont(0.9) within group (order by t1.day_rr) value_90per_rr,
	percentile_cont(0.1) within group (order by t1.day_rr) value_10per_rr,
	div(extract(year from t1.date), 10) * 10 decade
from
(select 
	avg(ebr.rr) day_rr,
	ebr.date
from eca_blend_rr ebr 
inner join stations s 
on ebr.staid = s.id
where s.cn = 'DE' and ebr.q_rr = 0
group by ebr.date) t1
group by div(extract(year from t1.date), 10) * 10) t2
full outer join
(select count(distinct(ebr2.date)) days_missing_values, div(extract(year from ebr2.date), 10) * 10 decade
from eca_blend_rr ebr2 
inner join stations s 
on s.id = ebr2.staid
where s.cn = 'DE' and ebr2.q_rr != 0
group by  div(extract(year from ebr2.date), 10)) t3
on t2.decade = t3.decade
inner join
(select count(distinct(ebr3.date)) total_days_counted, div(extract(year from ebr3.date), 10) * 10 decade
from eca_blend_rr ebr3
inner join stations s
on ebr3.staid = s.id
where s.cn = 'DE'
group by div(extract(year from ebr3.date), 10)) t4
on t4.decade = t3.decade) table_rr
on table_tg.decade_tg = table_rr.decade_rr
```
execution time in a AirMacBook with M1 processor 7 mins and 11 secs

<h2>Task 3</h2>

First at this task we are selecting our data grouped by the wanted countries per day adding an extra column as a flag for the phenomenon that we are studying. In our case I set 0 for non rain day and 1 for rainy day. I also counted the 'suspicious' non valid value days as long as we are not interested about the precision of the rain. I named the extra column "rain flag" (t1)
Then I created a table like t1 with an extra column that has the result of the substract of rain_flag minus lag(rain_flag) ordered by dates. This column will show me where weather changes from rain to no rain or the revert. From the values of this column I will reach the dates where the phenomenon that I am studying changes. The dates where we have the switch, are the rows where the result of this substraction is -1 or 1. I named this column flag_switch_weather and the table (t2).
After that I select only the dates which they have flag_switch_weather value -1 or 1, which are the dates where the phenomenon changes, and I substract the date with the previous one. The result is the sum of days where the phenomenon happens cummulative (t3).
From t3 table I select the rows where rain_flag = 0 because I want the the sum of the raining previous days. Then I group my rows per decade and country selecting the max cumulative raining days.

```SQl
select 
	max(t3.continuous_days),
	t3.cn,
	div(extract(year from t3.date), 10) * 10 decade
from
(select 
	t2.date,
	t2.cn,
	t2.rain_flag,
	t2.flag_switch_weather,
	t2.date - lag(t2.date) over(partition by t2.cn order by t2.date) continuous_days 
from
(select
	t1.date,
	t1.cn,
	t1.rain_flag,
	t1.rain_flag - lag(t1.rain_flag) over(partition by t1.cn order by t1.date)   flag_switch_weather
from
(select 
	ebr.date,
	s.cn,
	sum(ebr.rr),
	case 
		when sum(ebr.rr) > 0 then 1
		else 0
	end rain_flag
from eca_blend_rr ebr 
inner join stations s
on ebr.staid = s.id 
where s.cn in ('GR', 'DE', 'FR')
group by s.cn, ebr.date) t1) t2
where t2.flag_switch_weather in (-1, 1)) t3
where t3.rain_flag = 0
group by t3.cn, div(extract(year from t3.date), 10) * 10
```
<h2>Task 4</h2>

First I create a table with the 90 percentile of the average temperature per decade for every country (t1).Then I use a subquery tha returns me the average temperature per day for every country, the date, the decade and the country. After that I use an inner join to combine t1 and t2 on decade so that I can practically add an extra column to the t2 with the 90 percentile per decade per country. This table is t3. Then I group by the t3 per decade and country and I use a case to count how many dates are above the 90 percentile of the decade.

```SQL
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
```
<h2>Task 5</h2>
For this task I used the command 'CREATE EXTENSION CUBE' and then the 'CREATE EXTENSION EARTHDISTANCE'. With these extensions I can use the '<@>' operator between two Points (data type) that counts the distance (in 2 dimensions) between them. Moreover I assumed a random pair of cordinates (30, 30) to answer to this task.
First I create the t1 table that returns me the closest station to the random point. Then I counted the monthly average temperature for this station. Then I created t3 table for the monthly average temperatures of the stations and I used inner join with t2 on the same month and year and for stations that are in radius 200 km from the station in t2. This join gave me t4. I grouped t4 per year per month to count the near stations and the average of their temperature.

```SQL
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
```
<h2>Task 6</h2>

I worked with same methodology, as the previous task, with the difference that now I am looking for the surrounding stations' values for every null value in a station. Maximum radius of the surrounding is 500km. So I select the dates and stations with null values and then I used an inner join on the same date and on distance between station less tha 500km. After that I grouped that by date and the stations that I am interested for counting the average of the stations within 500 km radius and creating cases for 50 km and 100km radius. After that I grouped that per month for every station. The problem was that I had memory issues in myAirMacBook (256 gb total memory and the rr table in the database is 24 gb) so I used two samples of data about 15% of the original size(tablesample system(15)). Execution time was 29m and 31s. 
Some observations from the result is that at least two averages are very close and in some cases the three averages are close enough. We could update the null values of the station with the result of the average of the three averages for this particular month, if we want to subsitude null values with a value hypothetically close to reality. It's just an hypothesis.

```SQl
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
from eca_blend_rr ebr tablesample system(30)
inner join stations s
on ebr.staid = s.id
where s.cn = 'DE' and rr is null) t1
inner join 
(select ebr2.rr, ebr2.date, ebr2.staid, s2.cords
from eca_blend_rr ebr2 tablesample system(30)
inner join stations s2
on ebr2.staid = s2.id
where q_rr = 0) t2
on t1.date = t2.date and (t1.cords<@>t2.cords) * 1.609 <= 500
group by t1.staid, t1.date
order by t1.staid, t1.date) t3
group by t3.staid, extract(year from t3.date), extract(month from t3.date)
```

<h2>Task 7</h2>

Executing the query below I chose germany because has almost 4 times more weather stations than the the second in stations country which is Norway. Not only that but Germany has 6 times more valid records about the rain than Norway. So there are a lot of data and a big spectrum of data having in mind the number of the stations.

```SQL
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
```
<h2>Task 8</h2>
Executing the query it returned me a column with the countries ordered by the average of the monthly average in descending order.Looking at the result we observe that Libanon has more than 2 times more rain in monthly average than the second country. This is a non expected result having in mind that Libanon is in middle east which is mostly a dry area. After Libanon from the second to seventh position the average is about the same. Almost all of the countries of the top ten don't belong to central europe. Moreover Norway is at ninth position of the higher monthly average of rain wich is concerning because it was expected mostly snowing than rain, cause of the country's location (close to arctic circle).

```SQL
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
order by monthly_avg desc, "year", "month") t1
group by t1.cn
order by avg(t1.monthly_avg) desc
```



