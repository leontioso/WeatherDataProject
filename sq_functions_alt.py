''' Loading files to the database alternatively file by file '''

import os
import psycopg2

def create_and_populate_ecatables(folder):
    variable_code = folder[-2:]
    files = os.listdir(folder)
    conn = psycopg2.connect('dbname=weatherdata user=leontioso password=postgres host=localhost')
    cur = conn.cursor()
    cur.execute(f''' create table eca_blend_{variable_code} (staid integer, souid integer, date date, {variable_code} integer, q_{variable_code} integer check( q_{variable_code} in (0, 1, 9)));''')
    for file in files:
        if file not in {'elements.txt', 'stations.txt', 'sources.txt'}:
            cur.execute(f'''copy eca_blend_{variable_code} (staid, souid, date, {variable_code}, q_{variable_code})
                            from program 'sudo runuser -l leontioso  -c "tail -q -n +22 /media/leontioso/data2/WeatherData/WeatherData/ECA_blend_{variable_code}/{file}'
                            delimiter ',' ''')
    conn.commit()
    print(f'eca_blend_{variable_code} table created')
    
def create_populate_sources(code_set):
    conn = psycopg2.connect('dbname=weatherdata user=leontioso password=postgres host=localhost')
    with conn:
        with conn.cursor() as cur:
            cur.execute('''create temp table sources_temp (source_row char(173));
                           create temp table sources_temp2 (staid integer, souid integer, souname varchar(40), cn varchar(2), cords Point,
						   hght smallint, elei varchar(4), start date, stop date, parid smallint, parnam varchar(51));
                           copy sources_temp (source_row)
                           from program 'sudo runuser -l postgres  -c "sudo tail -q -n +26 /home/leontioso/Desktop/WeatherData/*/sources.txt " ';
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
                           from sources_temp;
                           
                           create table sources (staid integer, souid integer, souname varchar(40), cn varchar(2), cords Point,
						   hght smallint, elei varchar(4), start date, stop date, parid smallint, parnam varchar(51));''')
            
            for weather_var_code in code_set:
                cur.execute(f'''
                            insert into sources (staid, souid, souname, cn, cords, hght, elei, start, stop, parid, parnam)
                            select select st2.staid, st2.souid, st2.souname, st2.cn, Point(st.cords[0], st.cords[1]), st2.hght, st2.elei, st2.start, st2.stop, st2.parid, st2.parnam
                            (select st.staid, st.souid, st.souname, st.cn, Point(st.cords[0], st.cords[1]) cords, st.hght, st.elei, st.start, st.stop, st.parid, st.parnam,
                            row_number() over(partition by st.staid, st.souid, st.elei order by st.staid, st.souid, st.elei) rn,
                            from sources_temp2 st
                            inner join eca_blend_{weather_var_code} eca_blend
                            on  st.staid = eca_blend.staid  and st.souid = eca_blend.souid  and eca_blend.date between st.start and st.stop
                            where substring(st.elei from 1 for 2) = '{weather_var_code.upper()}') st2
                            where st2.rn = 1
                            ''')
                print(f'Inserted values into table sources using eca_blend_{weather_var_code} table')
    conn.close()
    print('Sources table created and populated')


