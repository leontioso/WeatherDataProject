#! /bin/bash

#change the current directory
cd ~/Desktop/WeatherData
#make a folder for csvs
mkdir csv_files
#Starting with the eca tables 
for weather_var in $(ls ~/Desktop/WeatherData/ | cut -c 11-12)
do

tail -n +22 -q /home/leontioso/Desktop/WeatherData/ECA_blend_$weather_var/*_STAID*.txt > csv_files/ECA_blend_$weather_var.csv | echo "ECA_blend_$weather_var csv created" &
done

#creating the elements csv

tail -n +17 -q /home/leontioso/Desktop/WeatherData/*/elements.txt | cut -c 1-5 > csv_files/elements_1st.csv
tail -n +17 -q /home/leontioso/Desktop/WeatherData/*/elements.txt | cut -c 7-156 | tr , ' ' > csv_files/elements_2nd.csv
tail -n +17 -q /home/leontioso/Desktop/WeatherData/*/elements.txt | cut -c 158-172 > csv_files/elements_3rd.csv
paste -d ',' csv_files/elements_1st.csv csv_files/elements_2nd.csv csv_files/elements_3rd.csv | sort | uniq  > csv_files/elements.csv
rm csv_files/elements_*.csv

#creating the stations csv

tail -n +20 -q /home/leontioso/Desktop/WeatherData/*/stations.txt | sort | uniq > csv_files/stations.csv 

#creating sources

tail -n +26 -q /home/leontioso/Desktop/WeatherData/*/sources.txt | cut -c 1-12 > csv_files/sources_1st.csv
tail -n +26 -q /home/leontioso/Desktop/WeatherData/*/sources.txt | cut -c 14-53 | tr , ' ' > csv_files/sources_2nd.csv
tail -n +26 -q /home/leontioso/Desktop/WeatherData/*/sources.txt | cut -c 55-162 > csv_files/sources_3rd.csv
paste -d ',' csv_files/sources_1st.csv csv_files/sources_2nd.csv csv_files/sources_3rd.csv | sort | uniq  > csv_files/sources.csv
rm csv_files/sources_*.csv