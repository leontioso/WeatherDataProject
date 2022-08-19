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






