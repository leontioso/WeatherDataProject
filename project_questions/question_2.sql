select distinct on (t2.decade)
t2.total_counts_decade, t2.cumm_counts, t2.cumm_percent, t2.cumm_avg_temp, t2.decade
from
(select 
	t1.counts_of_temp_decade,
	t1.tg,
	t1.total_counts_decade,
	t1.perc_decade,
	t1.decade,
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
order by t2.decade, t2.cumm_percent desc



select distinct on (t2.decade)
t2.total_counts_decade, t2.cumm_counts, t2.cumm_percent, t2.cumm_avg_temp
from
(select 
	t1.counts_of_temp_decade,
	t1.tg,
	t1.total_counts_decade,
	t1.perc_decade,
	t1.decade,
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
order by t2.decade, t2.cumm_percent desc





