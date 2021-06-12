import json
import tempfile
from typing import Dict, Tuple, List

from datetime import datetime, timedelta
from dotenv import dotenv_values
import numpy as np
import pandas as pd
from pytz import timezone
import sqlalchemy
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, Integer, BigInteger
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import sessionmaker
from td.client import TDClient


configs = dotenv_values(".env")


# constants - data
BUCKET_NAME = configs["BUCKET_NAME"]
POSTGRES_CONNECTION_STRING = configs["POSTGRES_CONNECTION_STRING"]

# constants - services
TDA_CLIENT_ID = configs["TDA_CLIENT_ID"]
TDA_REDIRECT_URL = configs["TDA_REDIRECT_URL"]

# contants - financial
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
    creds_filename = "tda_api_creds.json"
    with open(creds_filename, "r") as f:
        creds_data = json.load(f)

    with tempfile.NamedTemporaryFile() as tmpfile:
        with open(tmpfile.name, "w") as f:
            json.dump(creds_data, f)

        # Create a new session, credentials path is required.
        TDSession = TDClient(
            client_id=TDA_CLIENT_ID,
            redirect_uri=TDA_REDIRECT_URL,
            credentials_path=tmpfile.name,
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

    return {
        "message": f"Successfully fetched option data for: {SYMBOLS}",
        "event": event,
    }


def transform_option_data_to_df(option_data) -> pd.DataFrame:
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
    cols_to_keep = [
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
    columns_to_drop = list(set(list(df.columns)) - set(cols_to_keep))

    df.drop(columns=columns_to_drop, inplace=True)

    cols_to_numeric = [
        "delta",
        "gamma",
        "theta",
        "rho",
        "volatility",
        "theoreticalOptionValue",
    ]

    for col in cols_to_numeric:
        df.loc[:, col] = df[col].replace(np.nan, 0)
        df.loc[:, col] = df[col].replace("NaN", 0)
        df.loc[:, col] = pd.to_numeric(df[col])

    def gen_s3_uri():
        tz = timezone("US/Eastern")
        now = datetime.now(tz)
        uri = f's3://{BUCKET_NAME}/finx-option-data/{now.year}/{now.month}/{now.day}/{now.strftime("%s")}.parquet.gzip'
        return uri

    s3_uri = gen_s3_uri()
    df.to_parquet(s3_uri, compression="gzip", index=False)

    return True


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


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

    for ids_chunk in chunks(ids, 50):
        dfs = []

        print(
            f"Moving ids to S3: {ids_chunk[0]}...{ids_chunk[-1]} (length={len(ids_chunk)})"
        )

        for _id in ids_chunk:
            option_data = session.query(OptionData).get(_id)
            if option_data.data is None:
                msg = f"--------------------------- WARNING - big deal - OptionData[id={option_data.id}] has no data"
                print(msg)
                continue

            df = transform_option_data_to_df(option_data)
            dfs.append(df)

        df = pd.concat(dfs)

        if write_df_to_s3(df):
            print(f"Deleting records from Postgres: {ids_chunk[0]}...{ids_chunk[-1]}")
            for _id in ids_chunk:
                session.query(OptionData).filter(OptionData.id == _id).delete()
                session.commit()

    return {
        "message": f"Successfully moved {len(ids)} rows from DB to S3 and deleted them",
        "event": event,
    }
