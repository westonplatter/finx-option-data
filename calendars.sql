select 
	
	ts.ticker_f
	, f.ticker
	, f.close as close_f
	, b.close as close_b
	, -f.close+b.close as close_diff
	, f.iv as iv_f
	, b.iv as iv_b
	, -f.iv+b.iv as iv_diff
	, f.exp_date as exp_date_f
	, b.exp_date as exp_date_b
	, b.dt as dt_b
	
from strategy_timespreads as ts
join option_quotes as f on ts.ticker_f = f.ticker and f.option_type = 'call' and f.strike = 460 and f.close is not null
join option_quotes as b on ts.ticker_b = b.ticker and b.option_type = 'call' and b.strike = 460 and b.close is not null
where
	ts.desc = 'fm3dte_calendar'
	and ts.dt = '2022-01-21 21:00:00+00'
	and f.dt = b.dt
	
order by b.dt

-- TODO - refill the table. There are errors for the fm3dte_calendar entries. 
/**

1-24/1-31 are wrong
1-28/2-04 are wrong
1-28/1-31 are right

**/

