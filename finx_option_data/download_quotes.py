from datetime import timedelta
from optparse import Option
import os
import time

import pandas_market_calendars as mcal
import pandas as pd
import numpy as np
from pytest import param
import requests
from sqlalchemy.sql import select
from sqlalchemy.orm import Session
from sqlalchemy import text
import sqlalchemy as sa

from finx_option_data.models import StockQuote, OptionQuote
from finx_option_data.configs import Config
from finx_option_data.data_ops import df_upsert, df_insert_do_nothing

from finx_option_pricer.option import Option


def gen_quotes_to_fetch(config: Config, ticker: str) -> None:
    with Session(config.engine_metrics) as session:
        nyse = mcal.get_calendar("NYSE")
        fetch_dates = nyse.schedule(start_date="2022-01-01", end_date="2022-6-1")

        for _, row in fetch_dates.iterrows():
            dt = row["market_close"]

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
    query = text(
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
    arr = np.array(np.meshgrid(strikes, option_types)).T.reshape(-1,2)
    return [[int(x), y] for x,y in arr]


def gen_option_quotes_to_fetch(config: Config, ticker: str) -> None:
    # TODO(weston) - change this query to detech the absent of a options data at start/end date with
    query = text(
        f"""select * from {StockQuote.__tablename__} where ticker = '{ticker}' order by dt asc limit 10"""
    )

    # configs
    MAX_DAYS_OUT = 70
    strike_distance = 20

    with config.engine_metrics.connect() as con:
        df = pd.read_sql(query, con)

    opdfs = []

    for _, row in df.iterrows():
        strikes = list(
            np.array([x for x in range(-strike_distance, strike_distance)])
            + np.array([int(row["close"])])
        )

        for strike, option_type in strike_put_call_combinations(strikes):
            row_date = row["dt"].date()
            expiration_date_max = row_date + timedelta(days=MAX_DAYS_OUT)
            underlying_ticker = row["ticker"]

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
                ddf.rename(columns={"strike_price":"strike", "contract_type": "option_type", "expiration_date": "exp_date"}, inplace=True)
                ddf.exp_date = pd.to_datetime(ddf.exp_date)
                ddf.drop(columns=["cfi", "exercise_style", "primary_exchange", "shares_per_contract"], axis=1, inplace=True)
                ddf["dt"] = row["dt"]
                opdfs.append(ddf)

                print(".", end="", flush=True)

        ins_df = pd.concat(opdfs)
        df_insert_do_nothing(ins_df, config.engine_metrics, OptionQuote)


def gen_option_quotes_next(config: Config) -> None:
    query = text(
        f"""select * from {OptionQuote.__tablename__} where (fetched = false or fetched is null) order by dt desc limit 50"""
    )

    # TODO(weston) - let's look up all spot prices at the beginning and then fill pull from df
    stock_price_query = text(
        f"""select close from {StockQuote.__tablename__} where dt = :dt and ticker = :underlying_ticker"""
    )

    prices = {}

    with config.engine_metrics.connect() as con:
        df = pd.read_sql(query, con)

        for ix, row in df.iterrows():
            # gen url
            ticker = row["ticker"]
            date = row["dt"].date().strftime('%Y-%m-%d')
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
                
                spot = None
                key = (row["dt"], row["underlying_ticker"])
                if key in prices:
                    spot = prices[key]
                else:
                    result = pd.read_sql(stock_price_query, con, params=dict(dt=row["dt"], underlying_ticker=row["underlying_ticker"]))
                    spot = result.close.values[0]
                    prices[key] = spot
                    
                row['dte'] = (row['exp_date'] - row['dt'].date()).days
                dte_frac = row['dte'] / 252
                option = Option(S=spot, K=row["strike"], T=dte_frac, r=0.0025, sigma=None)
                row['iv'] = option.iv(row.close)
                option = Option(S=spot, K=row["strike"], T=dte_frac, r=0.0025, sigma=row['iv'])
                # everything based on IV 
                row['delta'] = option.delta
                row['theta'] = option.theta
                row['gamma'] = option.gamma
                row['vega'] = option.vega
                # replace row with updated values
                df.loc[ix] = row
                print(".", end="", flush=True)


    df_upsert(df, config.engine_metrics, OptionQuote)



if __name__ == "__main__":
    file_dir = os.path.dirname(os.path.realpath("__file__"))
    stage = os.getenv("STAGE", "prod").lower()
    file_name = f".env.{stage}"
    full_path = os.path.join(file_dir, f"./{file_name}")
    config = Config(full_path)

    # gen_quotes_to_fetch(config=config, ticker="SPY")
    # fetch_quotes_next(config=config)
    # gen_option_quotes_to_fetch(config=config, ticker="SPY")
    gen_option_quotes_next(config=config)
