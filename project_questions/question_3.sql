/*
-First at this task we are selecting our data grouped by the wanted countries per day adding an extra column 
as signed for the phenomenon that we are studying. In our case I set 0 for non rain day and 1 for rainy day. I also 
counted the 'suspicious' non valid value days as long as we are not intrested about the precision. I named the extra column
"rain flag" (t1)
-Then I created a table like t1 with an extra column that has the result of the substract of rain_flag minus lag(rain_flag)
ordered by dates. This will give me the signs where weather changes from rain to no rain or the revert ad from these signs I will
reach the dates where the phenomenon that I am studying changes. The signs are the rows where the result of the substract is
-1 or 1. I named this column flag_switch_weather and the table (t2).
-After that I select only the dates who have flag_switch_weather value -1 or 1, which are the dates where the phenomenon changes.
and I substract the date with the previous one. The result is the sum of days where the phenomenon happens cummulative (t3).
-From t3 table i select the rows where rain_flag = 0 because i want the the sum of the raining previous days. Then i group my rows
per decade and country selecting the max cumulative raining days.
*/
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






