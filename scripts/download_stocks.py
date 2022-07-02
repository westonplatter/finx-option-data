import pandas as pd

from finx_option_data.configs import Config

config = Config(".env.prod")

from finx_option_data.stock_quote_fetch_agent import StockQuoteFetchAgent

sagent = StockQuoteFetchAgent(
    polygon_api_key=config.polygon_api_key, engine=config.engine_metrics
)

from finx_option_data.utils import chunks

for dates in chunks(pd.date_range(start="2022-1-3", end="2022-3-1", freq="D"), 20):
    for ticker in ["SPY"]:
        # for ticker in ["SPY", "USO", "QQQ", "IWM"]:
        sd, ed = dates[0], dates[-1]
        sagent.ingest_price(ticker, sd, ed)
    print(f"\nstart={sd}, end={ed}")
