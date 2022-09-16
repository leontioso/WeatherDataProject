#! /bin/bash

#change the current directory
cd ~/Desktop/WeatherData
#make a folder for csvs
mkdir csv_files
#Starting with the eca tables 
array_weather_var=() 
for weather_var in $(ls ~/Desktop/WeatherData/ | cut -c 11-12)
do
tail -n +22 -q /home/leontioso/Desktop/WeatherData/ECA_blend_$weather_var/*_STAID*.txt > csv_files/ECA_blend_$weather_var.csv | echo "ECA_blend_$weather_var csv created" &
array_weather_var+=($weather_var)
done

#creating the elements csv

tail -n +17 -q /home/leontioso/Desktop/WeatherData/*/elements.txt | cut -c 1-5 > csv_files/elements_1st.csv
tail -n +17 -q /home/leontioso/Desktop/WeatherData/*/elements.txt | cut -c 7-156 | tr , ' ' > csv_files/elements_2nd.csv
tail -n +17 -q /home/leontioso/Desktop/WeatherData/*/elements.txt | cut -c 158-172 > csv_files/elements_3rd.csv
paste -d ',' csv_files/elements_1st.csv csv_files/elements_2nd.csv csv_files/elements_3rd.csv | sort | uniq  > csv_files/elements.csv
rm csv_files/elements_*.csv

#creating the stations csv

tail -n +20 -q /home/leontioso/Desktop/WeatherData/*/stations.txt | sort | uniq > csv_files/stations.csv 

#creating sources csv

tail -n +26 -q /home/leontioso/Desktop/WeatherData/*/sources.txt | cut -c 1-12 > csv_files/sources_1st.csv;
tail -n +26 -q /home/leontioso/Desktop/WeatherData/*/sources.txt | cut -c 14-53 | tr , ' ' > csv_files/sources_2nd.csv;
tail -n +26 -q /home/leontioso/Desktop/WeatherData/*/sources.txt | cut -c 55-162 > csv_files/sources_3rd.csv;
paste -d ',' csv_files/sources_1st.csv csv_files/sources_2nd.csv csv_files/sources_3rd.csv | sort | uniq | tr '\r' 'r' > csv_files/sources.csv;
rm csv_files/sources_*.csv

#create tables in weatherdata database
for weather_var in ${array_weather_var[@]}
do
echo "Creating weather data table $weather_var"
psql weatherdata -c "CREATE TABLE eca_blend_$weather_var (staid integer, souid integer, date date, $weather_var integer, q_$weather_var integer check( q_$weather_var in (0, 1, 9)));  " | echo "ECA_blend_$weather_var created"
done

#inserting into psql database weatherdata
echo inserting values to tables
for weather_var in ${array_weather_var[@]}
do
cat /home/leontioso/Desktop/WeatherData/csv_files/ECA_blend_tg.csv | psql weatherdata -c "COPY ECA_blend_$weather_var FROM STDIN (DELIMITER ',');" | echo values inserted to eca_table_$weather_var &
done

#create table stations
echo creating table stations
psql weatherdata -c 'create table stations_temp (id integer, staname varchar(40), cn varchar(2),lat varchar(9), lon varchar(10), hght smallint)';
cat /home/leontioso/Desktop/WeatherData/csv_files/stations.csv | psql weatherdata -c "COPY stations_temp FROM STDIN (DELIMITER ',');"
psql weatherdata -f ~/Desktop/WeatherData/sql_quarries/create_stations.sql

#create table elements
echo creating table elements
psql weatherdata -c 'create table elements_temp (id varchar(5), "desc" varchar(150), unit varchar(15)) ;'
cat /home/leontioso/Desktop/WeatherData/csv_files/elements.csv | psql weatherdata -c "COPY elements_temp FROM STDIN (DELIMITER ',');"
psql weatherdata -f ~/Desktop/WeatherData/sql_quarries/create_elements.sql

#create table sources
echo creating table sources
psql weatherdata -c 'create table sources_temp (staid integer, souid integer, souname varchar(40), cn varchar(2), lat varchar(9), lon varchar(10),
						   hght smallint, elei varchar(4), start date, stop date, parid varchar(5), parnam varchar(51));'

iconv -f ISO-8859-1 -t UTF-8 csv_files/sources.csv | psql weatherdata -c "COPY sources_temp FROM STDIN (DELIMITER ',');" 
psql weatherdata -f ~/Desktop/WeatherData/sql_quarries/create_sources.sql
