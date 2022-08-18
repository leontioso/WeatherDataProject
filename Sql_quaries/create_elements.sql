create temp table elements_temp (element_row varchar(172)) ;
create table elements (id varchar(5), "desc" varchar(150), unit varchar(15));

copy elements_temp (element_row)
from program E'tail -q -n +22 /Users/leontiosorfanos/Desktop/SqlProjects/WeatherData/*/elements.txt';


insert into elements (id, "desc", unit)
SELECT
	trim(SUBSTRING(element_row from 1 for 5)),
	trim(SUBSTRING(element_row from 7 for 150)),
	trim(SUBSTRING(element_row from 158 for 15))
FROM elements_temp;


alter table elements
add primary key (id)