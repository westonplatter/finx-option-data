with fm as (
	select 
		ts.dt
		, ts.desc
		, f.close as close_f
		, b.close as close_b
		, -f.close+b.close as closs_diff
		, f.iv as iv_f
		, b.iv as iv_b
		, -f.iv+b.iv as iv_diff
		, f.dte
		, f.strike
		, f.option_type
		, f.exp_date as exp_date_f
		, b.exp_date as exp_date_b
		
	from strategy_timespreads as ts
	join option_quotes as f on f.id = ts.id_f
	join option_quotes as b on b.id = ts.id_b
)

select * 
from fm
where
	fm.strike in (440, 445)
	and fm.option_type = 'call'
	and fm.exp_date_f = '2022-02-11'
order by strike, dt, dte
;


select exp_date, strike 
from option_quotes as o 
where 
	o.exp_date_weekday = 4
	and dte > 30
group by o.exp_date, o.strike
order by o.exp_date
;

/**

user query 2 to find exp_date / strike combos
loop over those
and run query 1
to generate 3d_cal spread vintages
run regression on them

**/
