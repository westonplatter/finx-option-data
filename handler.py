import json
import logging
import os
import sys
import tempfile
import time
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple

import requests
import numpy as np
import pandas as pd
import sqlalchemy
from dotenv import dotenv_values
from loguru import logger
from pytz import timezone
from sqlalchemy import BigInteger, Column, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, sessionmaker
from tda.auth import client_from_token_file
from tda.client import Client

from helpers import get_aws_secret, get_heroku_config, set_aws_secret


configs = dotenv_values(".env")


# constants - data
BUCKET_NAME: str = configs["BUCKET_NAME"]
POSTGRES_CONNECTION_STRING: str = configs["POSTGRES_CONNECTION_STRING"]
CHUNKS_COUNT: int = int(configs.get("CHUNKS_COUNT", "100"))

# constants - services
DISCORD_CHANNEL_URL: str = configs["DISCORD_CHANNEL_URL"]
DISCORD_LOG_FETCH = configs.get("DISCORD_LOG_FETCH", "False") == "True"
DISCORD_LOG_MOVE = configs.get("DISCORD_LOG_MOVE", "False") == "True"
TDA_CLIENT_ID: str = configs["TDA_CLIENT_ID"]
TDA_REDIRECT_URL: str = configs["TDA_REDIRECT_URL"]
TDA_CREDENTIALS_FILE_NAME: str = "tda_api_creds.json"


# constants - financial
OPTIONS_SCAN_SYMBOLS: List[str] = configs.get(
    "OPTIONS_SCAN_SYMBOLS", "SPY,QQQ,TLT,AMZN,XLE,XLK,AAPL,USO"
).split(",")
OPTIONS_SCAN_MAX_DTE: int = int(configs.get("OPTIONS_SCAN_MAX_DTE", "60"))


# DB models
Base = declarative_base()


def setup_logging():
    logger.remove()

    stage_name = os.getenv("STAGE", "dev")
    log_level = logging.INFO if stage_name == "prod" else logging.DEBUG

    logger.add(sys.stderr, level=log_level)


class OptionData(Base):
    __tablename__ = "option_data"

    id = Column(Integer, primary_key=True)
    symbol = Column(String)
    quote_time = Column(BigInteger)
    data = Column(JSONB)

    def __repr__(self):
        return f"<OptionData(symbol={self.symbol} quote_time={self.quote_time})>"


def create_engine() -> sqlalchemy.engine:
    def gen_engine() -> sqlalchemy.engine:
        # TODO(weston) - swap out prod for stage name
        aws_sn = "finx-option-data/prod/config"
        aws_config = json.loads(get_aws_secret(secret_name=aws_sn))

        # to resolve a db dialect issue
        db_url = aws_config["DATABASE_URL"].replace("postgres", "postgresql")
        engine = sqlalchemy.create_engine(db_url)
        return engine

    try:
        return gen_engine()
    except Exception:
        handler_check_pg_password(None, None)
    finally:
        return gen_engine()


def gen_tda_client() -> Client:
    with open(TDA_CREDENTIALS_FILE_NAME, "r") as f:
        credentials_data = json.load(f)

    with tempfile.NamedTemporaryFile() as tmpfile:
        with open(tmpfile.name, "w") as f:
            json.dump(credentials_data, f)

        return client_from_token_file(tmpfile.name, TDA_CLIENT_ID)


def store_data(session, option_chain) -> None:
    symbol: str = option_chain["underlying"]["symbol"]
    quote_time: int = option_chain["underlying"]["quoteTime"]
    data: Dict = option_chain

    option_data = OptionData(symbol=symbol, quote_time=quote_time, data=data)

    session.add(option_data)
    session.commit()


def gen_from_and_to_dates() -> Tuple[date, date]:
    tz = timezone("US/Eastern")
    now = datetime.now(tz)
    to_date = now + timedelta(days=31)
    return (now.date(), to_date.date())


def fetch_data(tda_client: Client, symbol: str) -> Dict:
    from_date, to_date = gen_from_and_to_dates()

    default_params: Dict = {
        "contract_type": Client.Options.ContractType.ALL,
        "strike_count": 50,
        "include_quotes": "TRUE",
        "strike_range": Client.Options.StrikeRange.NEAR_THE_MONEY,
        "from_date": from_date,
        "to_date": to_date,
    }
    response = tda_client.get_option_chain(symbol, **default_params)
    oc = response.json()
    logger.debug(f"fetched [{symbol}]")
    return oc


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


def discord_post_message(message: str) -> None:
    requests.post(DISCORD_CHANNEL_URL, json={"content": message})


def handler_fetch_data(event, context):
    setup_logging()

    tda_client = gen_tda_client()

    option_chains = []
    for symbol in OPTIONS_SCAN_SYMBOLS:
        option_chain = fetch_data(tda_client, symbol)
        option_chains.append(option_chain)

    engine = create_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    for oc in option_chains:
        symbol: str = oc["underlying"]["symbol"]
        store_data(session, oc)
        logger.debug(f"Storing option chain [{symbol}] - finished")

    message = f"Successfully fetched option data for: {OPTIONS_SCAN_SYMBOLS}"
    if DISCORD_LOG_FETCH:
        discord_post_message(message=message)
    return {
        "message": message,
        "event": event,
    }


def handler_move_data_to_s3(event, context):
    setup_logging()

    engine = create_engine()
    Session = sessionmaker(bind=engine)
    session = Session()

    # display total count
    logger.info(f"Total OptionData records = {session.query(OptionData.id).count()}")

    sql_statement = f"select id from {OptionData.__tablename__}"

    ids = []
    with engine.connect() as con:
        results = con.execute(sql_statement)
        ids = [x[0] for x in results]

    if len(ids) == 0:
        logger.info("No OptionData instances to move to S3")
        return None

    for ids_chunk in chunks(ids, CHUNKS_COUNT):
        time_start = time.time()
        dfs = []

        msg = f"Moving ids to S3: {ids_chunk[0]}..{ids_chunk[-1]} (length={len(ids_chunk)})"
        logger.debug(msg)

        for _id in ids_chunk:
            option_data = session.query(OptionData).get(_id)
            if option_data.data is None:
                msg = f"--------------------------- WARNING - big deal - OptionData[id={option_data.id}] has no data"
                logger.warning(msg)
                continue

            df = transform_option_data_to_df(option_data)
            del option_data
            dfs.append(df)

        df = pd.concat(dfs)
        del dfs

        if write_df_to_s3(df):
            del df
            logger.debug(
                f"Deleting records from Postgres: {ids_chunk[0]}..{ids_chunk[-1]}"
            )
            for _id in ids_chunk:
                session.query(OptionData).filter(OptionData.id == _id).delete()
                session.commit()

        time_diff = time.time() - time_start
        msg = f"Processed {len(ids_chunk)} records in {time_diff:.2f} seconds"
        logger.info(msg)

    message = f"Successfully moved {len(ids)} rows from DB to S3 and deleted them"
    if DISCORD_LOG_MOVE:
        discord_post_message(message)
    return {"message": message, "event": event}


def handler_check_pg_password(event, context):
    # TODO(weston) - swap out prod for stage name
    aws_sn = "finx-option-data/prod/config"
    aws_config = json.loads(get_aws_secret(secret_name=aws_sn))

    # pass stage into function to separate out prod/dev/etc
    heroku_config = get_heroku_config(
        heroku_app_name="finx-option-data", aws_secret_name="finx-option-data/heroku"
    )

    if aws_config["DATABASE_URL"] != heroku_config["DATABASE_URL"]:
        data = {"DATABASE_URL": heroku_config["DATABASE_URL"]}
        set_aws_secret(secret_name=aws_sn, json_data=data)
        discord_post_message("Successfully updated the Postgres Password")
