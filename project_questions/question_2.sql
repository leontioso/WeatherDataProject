select 3652 - t_valid_days.days_with_nonull days_with_nonvalid_value,t_valid_days.days_with_nonull, t_valid_days.decade decade_valid, t90per.decade decade90, t10per.decade decade10, t90per.total_counts_decade, t90per.cumm_counts cumm_counts_90, t90per.cumm_avg_temp cumm_avg_temp_90,
		t10per.cumm_counts cumm_counts_10, t10per.cumm_avg_temp cumm_avg_temp_10
from
(select count(*) days_with_nonull, div(extract(year from t4.date_with_nonull), 10) * 10 as decade
from
(select t3.date_with_nonull from 
(select  date date_with_nonull from eca_blend_tg ebt3 
inner join sources s3 
on s3.staid = ebt3.staid and s3.souid = ebt3.souid and substring(s3.elei from 1 for 2) = 'TG' and cn = 'DE'
group by date) t3
where t3.date_with_nonull not in
(select  date date_with_null from eca_blend_tg ebt4 
inner join sources s4 
on s4.staid = ebt4.staid and s4.souid = ebt4.souid and substring(s4.elei from 1 for 2) = 'TG' and cn = 'DE'
where ebt4.tg is null or ebt4.q_tg != 0
group by date)) t4
group by div(extract(year from t4.date_with_nonull), 10) * 10) t_valid_days
full outer join
(select distinct on (t2.decade)
t2.total_counts_decade, t2.cumm_counts, t2.cumm_percent, t2.cumm_avg_temp, t2.decade
from
(select 
	t1.counts_of_temp_decade,
	t1.tg,
	t1.total_counts_decade,
	t1.perc_decade,
	t1.decade * 10 decade,
	sum(t1.counts_of_temp_decade) over(partition by t1.decade order by t1.perc_decade desc rows between unbounded preceding and current row) cumm_counts,
	sum(t1.perc_decade) over(partition by t1.decade order by t1.perc_decade desc rows between unbounded preceding and current row) cumm_percent,
	sum(t1.tg) over(partition by t1.decade order by t1.perc_decade desc rows between unbounded preceding and current row)/
	sum(t1.perc_decade) over(partition by t1.decade order by t1.perc_decade desc rows between unbounded preceding and current row) cumm_avg_temp
from
(select 
 count(ebt.tg) counts_of_temp_decade,
 sum(count(ebt.tg)) over(partition by div(extract(year from ebt.date), 10) order by div(extract(year from ebt.date), 10)) total_counts_decade,
 cast(count(ebt.tg) as real) / cast(sum(count(ebt.tg)) over(partition by div(extract(year from ebt.date), 10) order by div(extract(year from ebt.date), 10)) as real) perc_decade,
 ebt.tg, 
 div(extract(year from ebt.date), 10) decade
from eca_blend_tg ebt
inner join sources s 
on ebt.staid = s.staid and ebt.souid = s.souid and substring(s.elei from 1 for 2) = 'TG' and ebt.q_tg = 0 and s.cn='DE'
group by ebt.tg, div(extract(year from ebt.date), 10)
order by decade, counts_of_temp_decade desc) t1) t2
where t2.cumm_percent <= 0.9
order by t2.decade, t2.cumm_percent desc) t90per
on t_valid_days.decade = t90per.decade
full outer join
(select distinct on (t2.decade)
t2.total_counts_decade, t2.cumm_counts, t2.cumm_percent, t2.cumm_avg_temp,t2.decade
from
(select 
	t1.counts_of_temp_decade,
	t1.tg,
	t1.total_counts_decade,
	t1.perc_decade,
	t1.decade * 10 decade,
	sum(t1.counts_of_temp_decade) over(partition by t1.decade order by t1.perc_decade desc rows between unbounded preceding and current row) cumm_counts,
	sum(t1.perc_decade) over(partition by t1.decade order by t1.perc_decade desc rows between unbounded preceding and current row) cumm_percent,
	sum(t1.tg) over(partition by t1.decade order by t1.perc_decade desc rows between unbounded preceding and current row)/
	sum(t1.perc_decade) over(partition by t1.decade order by t1.perc_decade desc rows between unbounded preceding and current row) cumm_avg_temp
from
(select 
 count(ebt.tg) counts_of_temp_decade,
 sum(count(ebt.tg)) over(partition by div(extract(year from ebt.date), 10) order by div(extract(year from ebt.date), 10)) total_counts_decade,
 cast(count(ebt.tg) as real) / cast(sum(count(ebt.tg)) over(partition by div(extract(year from ebt.date), 10) order by div(extract(year from ebt.date), 10)) as real) perc_decade,
 ebt.tg, 
 div(extract(year from ebt.date), 10) decade
from eca_blend_tg ebt
inner join sources s 
on ebt.staid = s.staid and ebt.souid = s.souid and substring(s.elei from 1 for 2) = 'TG' and ebt.q_tg = 0 and s.cn='DE'
group by ebt.tg, div(extract(year from ebt.date), 10)
order by decade, counts_of_temp_decade desc) t1) t2
where t2.cumm_percent <= 0.1
order by t2.decade, t2.cumm_percent desc) t10per
on t_valid_days.decade = t10per.decade





