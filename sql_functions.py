import psycopg2
import concurrent.futures
from functools import partial

def create_populate_eca_table(weather_var_code):
    conn = psycopg2.connect('dbname=weatherdata user=postgres password=postgres host=localhost')
    with conn:
        with conn.cursor() as cur:
            print(f'Creating eca table {weather_var_code}')
            cur.execute(f'''create table eca_blend_{weather_var_code} (staid integer, souid integer, date date, {weather_var_code} integer, q_{weather_var_code} integer check( q_{weather_var_code} in (0, 1, 9)));                
                copy eca_blend_{weather_var_code} (staid, souid, date, {weather_var_code}, q_{weather_var_code})
                from program 'tail -q -n +22 /home/leontioso/Desktop/WeatherData/ECA_blend_{weather_var_code}/{weather_var_code.upper()}_STAID* '
                 delimiter ','
                 ''')
    conn.close()
    return f'eca_blend_{weather_var_code} table created'
                
def create_populate_sources(code_set):
    conn = psycopg2.connect('dbname=weatherdata user=postgres password=postgres host=localhost')
    with conn:
        with conn.cursor() as cur:
            cur.execute('''create temp table sources_temp (staid integer, souid integer, souname varchar(40), cn varchar(2), lat varchar(9), lon varchar(10),
						   hght smallint, elei varchar(4), start date, stop date, parid varchar(5), parnam varchar(51));

                           create temp table sources_temp2 (staid integer, souid integer, souname varchar(40), cn varchar(2), cords Point,
						   hght smallint, elei varchar(4), start date, stop date, parid smallint, parnam varchar(51));

                           copy sources_temp (staid, souid, souname, cn, lat, lon, hght, elei, start, stop, parid, parnam)
                           from program 'cat /home/leontioso/Desktop/WeatherData/csv_files/sources.csv'
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
            
            with concurrent.futures.ThreadPoolExecutor() as executor:
                partial_combine = partial(combine_sources_eca_tables, cur=cur)
                results = executor.map(partial_combine, code_set)
                for result in results:
                    print(result)
                    
          
    conn.close()
    
    
def combine_sources_eca_tables(code, cur):
    cur.execute(f'''
                            insert into sources (staid, souid, souname, cn, cords, hght, elei, start, stop, parid, parnam)
                            select 
                            st2.staid, st2.souid, st2.souname, st2.cn, Point(st2.cords[0], st2.cords[1]), st2.hght, st2.elei, st2.start, st2.stop, st2.parid, st2.parnam
                            from
                            (select st.staid, st.souid, st.souname, st.cn, Point(st.cords[0], st.cords[1]) cords, st.hght, st.elei, st.start, st.stop, st.parid, st.parnam,
                            row_number() over(partition by st.staid, st.souid, st.elei order by st.staid, st.souid, st.elei) rn
                            from sources_temp2 st
                            inner join eca_blend_{code} eca_blend
                            on  st.staid = eca_blend.staid  and st.souid = eca_blend.souid  and eca_blend.date between st.start and st.stop
                            where substring(st.elei from 1 for 2) = '{code.upper()}') st2
                            where st2.rn = 1
                            ''')
    return f'eca_blend_{code} combined'       
    
        
    
def create_populate_elements():
    conn = psycopg2.connect('dbname=weatherdata user=postgres password=postgres host=localhost')
    with conn:
        with conn.cursor() as cur:
            cur.execute('''
                        create temp table elements_temp (element_row varchar(172)) ;
                        create table elements (id varchar(5), "desc" varchar(150), unit varchar(15));

                        copy elements_temp (element_row)
                        from program 'cat /home/leontioso/Desktop/WeatherData/csv_files/elements.csv';

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
                        from program 'cat /home/leontioso/Desktop/WeatherData/csv_files/stations.csv'
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
    
def update_eca_tables_withnull(code):
    conn = psycopg2.connect('dbname=weatherdata user=postgres password=postgres host=localhost')
    with conn:
        with conn.cursor() as cur:
            cur.execute(f'''update eca_blend_{code}
                                    set {code} = null
                                    where {code} = -9999
                                ''')
            print(f'Eca_table_{code} updated') 
    conn.close()