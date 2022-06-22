from datetime import timedelta, date, datetime
from optparse import Option
import os
from re import L
import time
from typing import List

import pandas as pd
import numpy as np
from pytest import param
import requests
from sqlalchemy.orm import Session
import sqlalchemy as sa

from finx_option_pricer.option import Option

from finx_option_data.polygon_helpers import reference_options_contracts, open_close
from finx_option_data.models import StockQuote, OptionQuote
from finx_option_data.configs import Config
from finx_option_data.data_ops import df_upsert, df_insert_do_nothing
from finx_option_data.utils import _market_days_between

# TODO(weston) - let's look up all spot prices at the beginning and then fill pull from df
stock_price_query = sa.text(
    f"""select close from {StockQuote.__tablename__} where dt = :dt and ticker = :underlying_ticker"""
)

def gen_quotes_to_fetch(config: Config, ticker: str) -> None:
    with Session(config.engine_metrics) as session:
        sd = pd.to_datetime("2022-01-01")
        ed = date.today()

        for dt in _market_days_between(sd, ed):
            results = session.execute(
                f"""
                    select count(id) 
                    from {StockQuote.__tablename__} as t
                    where t.ticker = :ticker and dt = :dt
                """,
                {"dt": dt, "ticker": ticker},
            )

            if results.fetchone()[0] == 0:
                st = StockQuote(ticker=ticker, dt=dt)
                session.add(st)
                session.commit()


def fetch_quotes_next(config: Config) -> None:
    query = sa.text(
        f"""select * from {StockQuote.__tablename__} where fetched = false order by dt limit 300"""
    )

    with config.engine_metrics.connect() as con:
        df = pd.read_sql(query, con)

    for ix, row in df.iterrows():
        # gen url
        date = row["dt"].date().strftime("%Y-%m-%d")
        ticker = row["ticker"]
        url = f"https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&apiKey={config.polygon_api_key}"
        # make request
        res = requests.get(url)
        # parse response
        if res.status_code == 200:
            data = res.json()
            row["open"] = data["open"]
            row["close"] = data["close"]
            row["high"] = data["high"]
            row["low"] = data["low"]
            row["volume"] = data["volume"]
            row["pre_market"] = data["preMarket"]
            row["after_market"] = data["afterHours"]
            row["fetched"] = True
            # replace row with updated values
            df.loc[ix] = row
            print(".", end="", flush=True)

        # to not hit the free API limit
        time.sleep(12)

    df_upsert(df, config.engine_metrics, StockQuote)


def strike_put_call_combinations(strikes):
    option_types = ["put", "call"]
    arr = np.array(np.meshgrid(strikes, option_types)).T.reshape(-1, 2)
    return [[int(x), y] for x, y in arr]


def gen_option_quotes_to_fetch(config: Config, ticker: str) -> None:
    # TODO(weston) - change this query to detect the absent of a options data at start/end date with
    query = sa.text(
        f"""
        select *
        from {StockQuote.__tablename__}
        where ticker = :ticker and dt > :dt
        order by dt asc
        limit 1000
        """)

    # configs
    MAX_DAYS_OUT = 70
    strike_distance = 100

    with config.engine_metrics.connect() as con:
        df = pd.read_sql(query, con, params={"ticker": ticker, "dt": pd.to_datetime("2022-05-14")})

    opdfs = []
    
    # each row is a day
    for _, row in df.iterrows():
        strikes = list(
            np.array([x for x in range(-strike_distance, strike_distance)])
            + np.array([int(row["close"])])
        )

        for strike, option_type in strike_put_call_combinations(strikes):
            row_date = row["dt"].date()
            expiration_date_max = row_date + timedelta(days=MAX_DAYS_OUT)
            underlying_ticker = row["ticker"]

            time.sleep(0.01)

            url = f"https://api.polygon.io/v3/reference/options/contracts?"
            url += f"underlying_ticker={underlying_ticker}"
            url += f"&contract_type={option_type}"
            url += f"&strike_price={strike}"
            url += f"&expired=false"
            url += f"&limit=30"
            url += f"&apiKey={config.polygon_api_key}"
            url += f"&as_of={row_date.strftime('%Y-%m-%d')}"
            url += f"&expiration_date.lte={expiration_date_max.strftime('%Y-%m-%d')}"
            res = requests.get(url)

            if res.status_code == 200:
                data = res.json()["results"]
                if data == []:
                    continue
                ddf = pd.DataFrame(data)
                ddf.rename(
                    columns={
                        "strike_price": "strike",
                        "contract_type": "option_type",
                        "expiration_date": "exp_date",
                    },
                    inplace=True,
                )
                ddf.exp_date = pd.to_datetime(ddf.exp_date)
                ddf.drop(
                    columns=[
                        "cfi",
                        "exercise_style",
                        "primary_exchange",
                        "shares_per_contract",
                    ],
                    axis=1,
                    inplace=True,
                )
                ddf["dt"] = row["dt"]
                opdfs.append(ddf)
                print(".", end="", flush=True)

        print(f"{row_date}", flush=True)

        ins_df = pd.concat(opdfs)
        df_insert_do_nothing(ins_df, config.engine_metrics, OptionQuote)


def gen_option_quotes_next(config: Config) -> None:
    query = sa.text(
        f"""
        select * from {OptionQuote.__tablename__} 
        where (fetched is null) and dt > '2022-02-01' and dt < '2022-03-01'
        order by dt desc limit 100
        """
    )

    prices = {}

    with config.engine_metrics.connect() as con:
        df = pd.read_sql(query, con)

        for ix, row in df.iterrows():
            # gen url
            ticker = row["ticker"]
            date = row["dt"].date().strftime("%Y-%m-%d")
            url = f"https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&apiKey={config.polygon_api_key}"
            # make request
            res = requests.get(url)
            # parse response
            if res.status_code == 404:
                row["fetched"] = False
            if res.status_code == 200:
                data = res.json()
                row["open"] = data["open"]
                row["close"] = data["close"]
                row["high"] = data["high"]
                row["low"] = data["low"]
                row["volume"] = data["volume"]
                row["pre_market"] = data["preMarket"]
                row["after_market"] = data["afterHours"]
                row["fetched"] = True

                spot = None
                key = (row["dt"], row["underlying_ticker"])
                if key in prices:
                    spot = prices[key]
                else:
                    result = pd.read_sql(
                        stock_price_query,
                        con,
                        params=dict(
                            dt=row["dt"], underlying_ticker=row["underlying_ticker"]
                        ),
                    )
                    spot = result.close.values[0]
                    prices[key] = spot

                row["dte"] = (row["exp_date"] - row["dt"].date()).days
                dte_frac = row["dte"] / 252
                option = Option(
                    S=spot, K=row["strike"], T=dte_frac, r=0.0025, sigma=None
                )
                # store iv as a number, not a percent
                row["iv"] = option.iv(row.close)*100.0
                option = Option(
                    S=spot, K=row["strike"], T=dte_frac, r=0.0025, sigma=row["iv"]
                )
                # everything based on IV
                row["delta"] = option.delta
                row["theta"] = option.theta
                row["gamma"] = option.gamma
                row["vega"] = option.vega
                # replace row with updated values
            df.loc[ix] = row
            print(".", end="", flush=True)

        df_upsert(df, config.engine_metrics, OptionQuote)



def fetch_option_quotes_vintage(config: Config, ticker: str, sd: pd.Timestamp, ed: pd.Timestamp):
    datas = []
    for option_type in ['call', 'put']:
        data = reference_options_contracts(
            config.polygon_api_key,
            underlying_ticker=ticker,
            option_type=option_type,
            strike=round(close, 0),
            as_of=dt,
            exp_date_lte=(dt + timedelta(days=60)),
        )
        datas.append(pd.DataFrame(data))
    
    # listed contracts
    df = pd.concat(datas)
    df.rename(columns={"strike_price": "strike", "contract_type": "option_type", "expiration_date": "exp_date"}, inplace=True)
    df.drop(columns=["cfi","exercise_style","primary_exchange","shares_per_contract",],axis=1,inplace=True,)
    df['dt'] = dt
    df.exp_date = pd.to_datetime(df.exp_date)
    df_insert_do_nothing(df, config.engine_metrics, OptionQuote)

    # set the week day on listed options
    df['exp_date_weekday'] = df.exp_date.dt.weekday

    max_exp_date_monday = df.query("exp_date_weekday.isin([0])").exp_date.max()
    

    # for date in pd.date_range(start=pd.to_datetime(dt.strftime("%Y-%m-%d")), end=pd.to_datetime(max_exp_date_monday.strftime("%Y-%m-%d"))):
    #     import pdb; pdb.set_trace()

    prices = []

    # find Monday and Friday options within listed contracts (ie, those within next 60 days)
    monday_friday_weekdays = [0, 4]
    xdf = df.query("exp_date_weekday.isin(@monday_friday_weekdays)").copy()
    otickers = tuple(xdf.ticker.to_list())

    import pdb; pdb.set_trace()

    query = sa.text('select * from option_quotes where ticker IN :tickers and dt = :dt')
    with config.engine_metrics.connect() as con:
        fdf = pd.read_sql(query, con, params=dict(tickers=otickers, dt=dt))

        upsert_rows = []
        for idx, row in fdf.iterrows():
            if abs(row['close']) != 0.0:
                continue

            data = open_close(config.polygon_api_key, row['ticker'], dt)

            # if there's no close data (ie, no trades for dt)
            if data is None:
                continue

            row["open"] = data["open"]
            row["close"] = data["close"]
            row["high"] = data["high"]
            row["low"] = data["low"]
            row["volume"] = data["volume"]
            row["pre_market"] = data["preMarket"]
            row["after_market"] = data["afterHours"]
            row["fetched"] = True

            spot = None
            key = (row["dt"], row["underlying_ticker"])
            if key in prices:
                spot = prices[key]
            else:
                result = pd.read_sql(
                    stock_price_query,
                    con,
                    params=dict(
                        dt=row["dt"], underlying_ticker=row["underlying_ticker"]
                    ),
                )
                spot = result.close.values[0]
                prices[key] = spot

            row["dte"] = (row["exp_date"] - row["dt"].date()).days
            dte_frac = row["dte"] / 252
            option = Option(
                S=spot, K=row["strike"], T=dte_frac, r=0.0025, sigma=None
            )
            # store iv as a number, not a percent
            row["iv"] = option.iv(row.close)*100.0
            option = Option(
                S=spot, K=row["strike"], T=dte_frac, r=0.0025, sigma=row["iv"]
            )
            # everything based on IV
            row["delta"] = option.delta
            row["theta"] = option.theta
            row["gamma"] = option.gamma
            row["vega"] = option.vega

            upsert_rows.append(row)
        

        if len(upsert_rows) > 0:
            upsert_df = pd.DataFrame(upsert_rows)
            df_upsert(upsert_df, config.engine_metrics, OptionQuote)    



def fetch_atm_vintage(config: Config, ticker: str, sd: pd.Timestamp, ed: pd.Timestamp) -> None:
    query = sa.text(f"select * from {StockQuote.__tablename__} where ticker = :ticker and :sd <= date(dt) and date(dt) <= :ed order by dt")
    with config.engine_metrics.connect() as con:
        sdf = pd.read_sql(query, con, params=dict(ticker=ticker, sd=sd, ed=ed))
    
    close = sdf.close.values[0]

    datas = []
    for option_type in ['call', 'put']:
        data = reference_options_contracts(
            config.polygon_api_key,
            underlying_ticker=ticker,
            option_type=option_type,
            strike=round(close, 0),
            as_of=sd,
            exp_date_lte=(sd + timedelta(days=60)),
        )
        datas.append(pd.DataFrame(data))
    
    df = pd.concat(datas)

    # listed contracts
    
    df.rename(columns={"strike_price": "strike", "contract_type": "option_type", "expiration_date": "exp_date"}, inplace=True)
    df.drop(columns=["cfi","exercise_style","primary_exchange","shares_per_contract",],axis=1,inplace=True,)
    df.exp_date = pd.to_datetime(df.exp_date)

    # [(dt, option ticker)]
    option_quotes_to_fetch = []

    prices = {}

    for _, srow in sdf[10:11].iterrows():
        dt = srow['dt']
        insert_df = df.copy()
        insert_df['dt'] = dt
        df_insert_do_nothing(insert_df, config.engine_metrics, OptionQuote)
        for _, row in insert_df.iterrows():
            option_quotes_to_fetch.append((dt, row.ticker))

    # query = sa.text('select * from option_quotes where ticker IN :tickers and dt IN :dts')
    query = sa.text(f"select * from {OptionQuote.__tablename__} where ticker IN :tickers and :sd <= date(dt) and date(dt) <= :ed order by dt")
    with config.engine_metrics.connect() as con:
        otickers = [x[1] for x in option_quotes_to_fetch]
        fdf = pd.read_sql(query, con, params=dict(tickers=tuple(otickers), sd=sd, ed=ed))
        
        upsert_rows = []
        for _, row in fdf.iterrows():
            
            if row['close'] is not None:
                continue
            
            # ticker = row['ticker']
            # date = row['dt'].strftime("%Y-%m-%d")
            # api_key = config.polygon_api_key
            # url = f"https://api.polygon.io/v1/open-close/{ticker}/{date}?adjusted=true&apiKey={api_key}"
            data = open_close(config.polygon_api_key, row['ticker'], row['dt'])

            # if there's no close data (ie, no trades for dt)
            if data is None:
                continue
        

            row["open"] = data["open"]
            row["close"] = data["close"]
            row["high"] = data["high"]
            row["low"] = data["low"]
            row["volume"] = data["volume"]
            row["pre_market"] = data["preMarket"]
            row["after_market"] = data["afterHours"]
            row["fetched"] = True

            spot = None
            key = (row["dt"], row["underlying_ticker"])
            if key in prices:
                spot = prices[key]
            else:
                result = pd.read_sql(
                    stock_price_query,
                    con,
                    params=dict(
                        dt=row["dt"], underlying_ticker=row["underlying_ticker"]
                    ),
                )
                spot = result.close.values[0]
                prices[key] = spot

            row["dte"] = (row["exp_date"] - row["dt"].date()).days
            dte_frac = row["dte"] / 252
            option = Option(
                S=spot, K=row["strike"], T=dte_frac, r=0.0025, sigma=None
            )
            # store iv as a number, not a percent
            row["iv"] = option.iv(row.close)*100.0
            option = Option(
                S=spot, K=row["strike"], T=dte_frac, r=0.0025, sigma=row["iv"]
            )
            # everything based on IV
            row["delta"] = option.delta
            row["theta"] = option.theta
            row["gamma"] = option.gamma
            row["vega"] = option.vega

            upsert_rows.append(row)
        

        if len(upsert_rows) > 0:
            upsert_df = pd.DataFrame(upsert_rows)
            df_upsert(upsert_df, config.engine_metrics, OptionQuote)    




def gen_fm_calendars(config, dt):
    from finx_option_data.transforms import gen_friday_and_following_monday
    from finx_option_data.models import StrategyTimespreads

    query = sa.text("select * from option_quotes where underlying_ticker = :underlying_ticker and date(dt) = :dt")
    df = pd.read_sql(query, config.engine_metrics, params={"dt": dt, "underlying_ticker": "SPY"})
    ddf = gen_friday_and_following_monday(df)
    df_insert_do_nothing(ddf, config.engine_metrics, StrategyTimespreads)



if __name__ == "__main__":
    file_dir = os.path.dirname(os.path.realpath("__file__"))
    stage = os.getenv("STAGE", "prod").lower()
    file_name = f".env.{stage}"
    full_path = os.path.join(file_dir, f"./{file_name}")
    config = Config(full_path)

    # gen_quotes_to_fetch(config=config, ticker="SPY")
    # fetch_quotes_next(config=config)
    # gen_option_quotes_to_fetch(config=config, ticker="SPY")

    # for i in range(0,10000):
    #     print(f"{i} fetching next 50 option quotes")
    #     gen_option_quotes_next(config=config)

    # sd = pd.to_datetime("2022-01-03")
    # ed = pd.to_datetime("2022-06-18")
    # for dt in _market_days_between(sd, ed):
    #     gen_fm_calendars(config, dt)

    fetch_atm_vintage(config, "SPY", sd=pd.to_datetime("2022-01-03"), ed=pd.to_datetime("2022-01-24"))
    fetch_atm_vintage(config, "SPY", sd=pd.to_datetime("2022-01-11"), ed=pd.to_datetime("2022-02-01"))

    # select ts.id, ts.id_b, b.id, b.iv as biv, ts.id_f, f.id, f.iv as fiv, f.iv-b.iv as ivdiff, date(ts.dt) as dt, f.exp_date, b.exp_date, b.close-f.close as pricediff, f.delta, f.strike
    # from strategy_timespreads as ts
    # join option_quotes as b on b.id = ts.id_b
    # join option_quotes as f on f.id = ts.id_f
    # where f.dte > 10 and f.delta > 0.4 and f.delta < 0.6 and f.option_type = 'call'
    # order by f.exp_date, f.strike
