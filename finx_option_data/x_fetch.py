from datetime import date, datetime, timedelta
import os
import pandas as pd
import sqlalchemy as sa

from finx_option_data.option_quote_fetch_agent import OptionQuoteFetchAgent
from finx_option_data.stock_quote_fetch_agent import StockQuoteFetchAgent
from finx_option_data.configs import Config

if __name__ == "__main__":
    print(__file__, "start")
    
    # engine
    engine = sa.create_engine("sqlite:///test.db")
    
    # finx option data config
    file_dir = os.path.dirname(os.path.realpath("__file__"))
    stage = os.getenv("STAGE", "prod").lower()
    file_name = f".env.{stage}"
    full_path = os.path.join(file_dir, f"./{file_name}")
    config = Config(full_path)

    # date
    dt = pd.to_datetime("2023-03-24")

    # stock agent - fetch close price
    # sagent = StockQuoteFetchAgent(config.polygon_api_key, engine, throttle_api_requests=True)
    # data = sagent.fetch_price("SPY", dt)
    # stock_close = round(data["close"], 0)
    stock_close = 396
    
    # option agent
    agent = OptionQuoteFetchAgent(config.polygon_api_key, engine, throttle_api_requests=True)

    # fetch contracts
    df = agent.fetch_options_contracts_df(dt, "SPY", "call", strike=stock_close, dte=7)
    df = df[df.expiration_date > dt]

    prices = []
    for i, row in df.iterrows():
        x = agent.fetch_option_quotes_between(ticker=row.ticker, sd=dt, ed=dt)
        print(".", end="")

        if len(x) > 0:
            xdf = pd.DataFrame(x)
            xdf["dt"] = pd.to_datetime(xdf['from'])
            # add contract data
            xdf["underlying_ticker"] = row.underlying_ticker
            xdf["expiration_date"] = pd.to_datetime(row.expiration_date)
            xdf["contract_type"] = row.contract_type
            xdf["strike_price"] = row.strike_price
            # remove fetch options data
            xdf.drop(columns=["from", "afterHours", "preMarket", "status"], inplace=True)
            xdf.rename(columns={"symbol": "ticker"}, inplace=True)
            prices.append(xdf)

    fdf = pd.concat(prices, ignore_index=True)
    
    dt = pd.to_datetime(fdf["dt"].values[-1])
    underlying_ticker = fdf["underlying_ticker"].values[0]
    file_name = f"{underlying_ticker}-{dt.date()}.csv"
    full_path = os.path.join(file_dir, f"./data/{file_name}")
    fdf.to_csv(full_path)

        


