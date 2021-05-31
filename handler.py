from typing import Dict, Tuple, List

from datetime import datetime, timedelta
from pytz import timezone
import sqlalchemy
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Integer, BigInteger
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from td.client import TDClient
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np


# TODO(weston) make this work with local dev env and aws lambda
from dotenv import dotenv_values

configs = dotenv_values(".env")

# constants
POSTGRES_CONNECTION_STRING = configs["POSTGRES_CONNECTION_STRING"]
TDA_CLIENT_ID = configs["TDA_CLIENT_ID"]
TDA_REDIRECT_URL = configs["TDA_REDIRECT_URL"]
BUCKET_NAME = configs["BUCKET_NAME"]

SYMBOLS: List[str] = ["SPY", "QQQ", "TLT", "AMZN", "XLE", "XLK", "AAPL", "USO"]


# DB models
Base = declarative_base()


class OptionData(Base):
    __tablename__ = "option_data"

    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    quote_time = Column(BigInteger)
    data = Column(JSONB)

    def __repr__(self):
        return f"<OptionData(symbol={self.symbol} quote_time={self.quote_time})>"


def create_engine() -> sqlalchemy.engine:
    connection_str = POSTGRES_CONNECTION_STRING
    engine = sqlalchemy.create_engine(connection_str)
    return engine


def create_session() -> TDClient:
    # TODO(weston) - make this work for aws lambda functions
    import os

    dirname = os.path.dirname(__file__)
    filename = os.path.join(dirname, "tda_api_creds.json")

    # Create a new session, credentials path is required.
    TDSession = TDClient(
        client_id=TDA_CLIENT_ID,
        redirect_uri=TDA_REDIRECT_URL,
        credentials_path=filename,
    )
    TDSession.login()
    return TDSession


def store_data(session, option_chain) -> None:
    symbol: str = option_chain["underlying"]["symbol"]
    quote_time: int = option_chain["underlying"]["quoteTime"]
    data: Dict = option_chain

    option_data = OptionData(symbol=symbol, quote_time=quote_time, data=data)

    session.add(option_data)
    session.commit()


def gen_from_and_to_dates() -> Tuple[str, str]:
    tz = timezone("US/Eastern")
    now = datetime.now(tz)
    to_date = now + timedelta(days=31)

    from_data = now.date().strftime("%Y-%m-%d")
    to_data = to_date.date().strftime("%Y-%m-%d")

    return (from_data, to_data)


def fetch_data(tda_session, symbol) -> Dict:
    from_data, to_data = gen_from_and_to_dates()

    default_params: Dict = {
        "contractType": "ALL",
        "strikeCount": 50,
        "includeQuotes": "TRUE",
        "range": "NTM",
        "fromData": from_data,
        "toDate": to_data,
    }

    params = {**default_params, **{"symbol": symbol}}
    oc = tda_session.get_options_chain(params)
    print(f"fetched [{symbol}]")
    return oc


def handler_fetch_data(event, context):
    # body = {
    #     "message": "Go Serverless v1.0! Your function executed successfully!",
    #     "input": event,
    # }

    # response = {"statusCode": 200, "body": json.dumps(body)}

    # return response

    # # Use this code if you don't use the http event with the LAMBDA-PROXY
    # # integration
    # """
    # return {
    #     "message": "Go Serverless v1.0! Your function executed successfully!",
    #     "event": event
    # }
    # """

    engine = create_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    tda_session = create_session()

    option_chains = []
    for symbol in SYMBOLS:
        option_chain = fetch_data(tda_session, symbol)
        option_chains.append(option_chain)

    for _, oc in enumerate(option_chains):
        symbol: str = oc["underlying"]["symbol"]
        print(f"Storing option chain [{symbol}] - started")
        store_data(session, oc)
        print(f"Storing option chain [{symbol}] - finished")


def transform_option_data_to_df(option_data):
    def extract_option_contracts(option_map):
        result = []
        for _, v in option_map.items():
            for _, vv in v.items():
                result.append(vv[0])
        return result

    # trying to be memory efficient
    # puts
    both = extract_option_contracts(option_data.data["putExpDateMap"])
    # calls
    both.extend(extract_option_contracts(option_data.data["callExpDateMap"]))

    return pd.DataFrame.from_records(both)


def write_df_to_s3(df) -> bool:
    cols = [
        "ask",
        "bid",
        "rho",
        "last",
        "mark",
        # 'mini',
        "vega",
        "delta",
        "gamma",
        "theta",
        "symbol",
        "askSize",
        "bidSize",
        "putCall",
        # 'lastSize',
        # 'lowPrice',
        # 'highPrice',
        "netChange",
        "openPrice",
        "timeValue",
        "tradeDate",
        "bidAskSize",
        "closePrice",
        # 'inTheMoney',
        # 'markChange',
        # 'multiplier',
        "volatility",
        "description",
        # 'nonStandard',
        "strikePrice",
        "totalVolume",
        # 'exchangeName',
        "openInterest",
        # 'isIndexOption',
        "percentChange",
        "expirationDate",
        # 'expirationType',
        "lastTradingDay",
        # 'settlementType',
        # 'deliverableNote',
        "quoteTimeInLong",
        "tradeTimeInLong",
        "daysToExpiration",
        "markPercentChange",
        "theoreticalVolatility",
        # 'optionDeliverablesList',
        "theoreticalOptionValue",
    ]
    df = df[cols]

    for col in [
        "delta",
        "gamma",
        "theta",
        "rho",
        "volatility",
        "theoreticalOptionValue",
    ]:
        df[col] = df[col].replace(np.nan, 0)
        df[col] = df[col].replace("NaN", 0)
        df[col] = pd.to_numeric(df[col])

    def gen_s3_uri():
        tz = timezone("US/Eastern")
        now = datetime.now(tz)
        uri = f's3://{BUCKET_NAME}/finx-option-data/{now.year}/{now.month}/{now.day}/{now.strftime("%s")}.parquet.gzip'
        return uri

    s3_uri = gen_s3_uri()
    df.to_parquet(s3_uri, compression="gzip", index=False)

    return True


def handler_move_data_to_s3(event, context):
    engine = create_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    sql_statement = f"select id from {OptionData.__tablename__}"

    ids = []
    with engine.connect() as con:
        results = con.execute(sql_statement)
        ids = [x[0] for x in results]

    if len(ids) == 0:
        print("No OptionData instances to move to S3")
        return None

    dfs = []
    print(f"Moving ids to S3: {ids}")
    for _id in ids:
        option_data = session.query(OptionData).get(_id)
        df = transform_option_data_to_df(option_data)
        dfs.append(df)

    df = pd.concat(dfs)

    if write_df_to_s3(df):
        print(f"Deleting records from Postgres: {ids}")
        for _id in ids:
            session.query(OptionData).filter(OptionData.id == _id).delete()
            session.commit()
