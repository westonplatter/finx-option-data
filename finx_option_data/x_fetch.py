from datetime import date, datetime, timedelta
import os
import pandas as pd
import sqlalchemy as sa

from finx_option_data.option_quote_fetch_agent import OptionQuoteFetchAgent
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

    # agent 
    agent = OptionQuoteFetchAgent(config.polygon_api_key, engine, throttle_api_requests=True)

    # fetch contracts
    dt = pd.to_datetime("2023-03-24")
    df = agent.fetch_options_contracts_df(dt, "SPY", "call", strike=400, dte=3)
    df = df[df.expiration_date > dt]

    prices = []
    for i, row in df.iterrows():
        x = agent.fetch_option_quotes_between(row.ticker, dt, row.expiration_date)

        if len(x) > 0:
            xdf = pd.DataFrame(x)
            xdf["dt"] = pd.to_datetime(xdf['from'])
            xdf.drop(columns=["from", "afterHours", "preMarket"], inplace=True)
            xdf.rename(columns={"symbol": "ticker"}, inplace=True)
            prices.append(xdf)

    import pdb; pdb.set_trace()

        


