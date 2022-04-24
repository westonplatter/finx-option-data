from pandas.core import series
import boto3
from datetime import date, timedelta
from dotenv import dotenv_values
import hashlib
from loguru import logger
import os
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import s3fs
from typing import List

from helpers import setup_logging, elapsed_timer


stage = os.getenv("STAGE", "prod").lower()
configs = dotenv_values(f".env.{stage}")


# constants - data
BUCKET_NAME: str = configs["BUCKET_NAME"]

# setup - logging
setup_logging()


def list_folders(s3_client, bucket_name: str, prefix: str):
    result = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
    prefixes = result.get("CommonPrefixes")
    if prefixes is None:
        return []
    folders = [o.get("Prefix") for o in prefixes]
    return folders


def fetch_df(s3_fs_client, folder: str) -> pd.DataFrame:
    with elapsed_timer() as get_diff:
        s3_folder_url = (f"s3://{BUCKET_NAME}/{folder}")[:-1]
        df = (
            pq.ParquetDataset(s3_folder_url, filesystem=s3_fs_client)
            .read_pandas()
            .to_pandas()
        )
        logger.debug(f"fetch_df took = {get_diff():.2f} seconds")

    return df


def write_df(s3_fs_client, file_name: str, df: pd.DataFrame) -> None:
    key = f"s3://{BUCKET_NAME}/{file_name}"
    table = pa.Table.from_pandas(df)
    file_name = f"{hashlib.md5(df.values.tobytes()).hexdigest()}.parquet"
    pq.write_to_dataset(
        table=table,
        root_path=key,
        filesystem=s3_fs_client,
        partition_filename_cb=lambda x: file_name,
    )


def convert_timems_to_datetime(series: pd.Series, tz: str = "US/Eastern") -> pd.Series:
    """Apply pd.to_dateime and tz_localize

    Args:
        series (pd.Series): series
        tz (str, optional): the TZ to localize the time to. Default to
                            'US/Eastern' where market data is sourced.
    Returns:
        pd.Series: series
    """
    result = pd.to_datetime(series, unit="ms").dt
    if tz is None:
        return result
    else:
        return result.tz_localize(tz=tz)


def convert_timems_to_date(series: pd.Series, tz: str = "US/Eastern") -> pd.Series:
    """Apply pd.to_datetime (similar to `convert_timems_to_datetime`) but only return the date

    Args:
        series (pd.Series): series
        tz (str, optional): Timezone. Defaults to 'US/Eastern'.

    Returns:
        pd.Series: the `date`
    """
    return convert_timems_to_datetime(series=series, tz=tz).dt.date


def convert_timems_to_weekday(series):
    return pd.to_datetime(series, unit="ms").dt.weekday


def transform_df(df: pd.DataFrame) -> pd.DataFrame:
    """Note - we mutate the in memory dataframe and return it"""

    # add underlying symbol
    df["underlying_symbol"] = df.symbol.str.split("_").str[0]

    # add strike
    df["strike"] = df.symbol.str.extract(r"_.*[C|P](.*)")

    # convert trade/quote time to Eastern Time
    df["trade_time"] = convert_timems_to_datetime(
        series=df["tradeTimeInLong"], tz="US/Eastern"
    )
    df["quote_time"] = convert_timems_to_datetime(
        series=df["quoteTimeInLong"], tz="US/Eastern"
    )

    df["sample_time"] = convert_timems_to_datetime(
        series=df["sampleTimeInLong"], tz="utc"
    ).dt.tz_convert(tz="US/Eastern")

    # expiration date transformations
    df["expiration_date"] = convert_timems_to_date(
        df["expirationDate"], tz="US/Eastern"
    )
    df["expiration_weekday"] = convert_timems_to_weekday(df.expirationDate)

    # set index on quote time
    df.set_index("sample_time", drop=False, inplace=True)

    # disregard data that's outside market hours, ET
    market_start_time = "09:30:00"
    market_stop_time = "16:00:00"
    df = df.between_time(market_start_time, market_stop_time)

    return df


def process_folder(s3_fs_client, folder: str, root_path: str):
    df = fetch_df(s3_fs_client=s3_fs_client, folder=folder)
    df = transform_df(df)

    # prep numerical columns for aggregation
    columns_to_aggregate = [
        "ask",
        "bid",
        "mark",
        "vega",
        "delta",
        "gamma",
        "theta",
        "putCall",
        "timeValue",
        "volatility",
        "expirationDate",
        "daysToExpiration",
        "theoreticalVolatility",
        "theoreticalOptionValue",
        "symbol",
        # added columns
        "expiration_weekday",
        "expiration_date",
        "strike",
        "underlying_symbol",
    ]
    agg_dict = {}
    for column in columns_to_aggregate:
        agg_dict[column] = "last"

    underlying_symbols = sorted(list(set(df.underlying_symbol.values)))

    for underlying_symbol in underlying_symbols:
        logger.debug(f"processing {underlying_symbol}")

        symbol_df = df.query("underlying_symbol == @underlying_symbol")

        underlying_symbol_df = (
            symbol_df.groupby([pd.Grouper(freq="1min"), "symbol"])
            .agg(agg_dict)
            .droplevel(1)
        )

        year, month, day = (
            underlying_symbol_df.index.year[0],
            underlying_symbol_df.index.month[0],
            underlying_symbol_df.index.day[0],
        )
        file_name = f"{root_path}/{year}/{month}/{day}/{underlying_symbol}"

        write_df(
            s3_fs_client=s3_fs_client, file_name=file_name, df=underlying_symbol_df
        )
        logger.debug(
            f"{underlying_symbol}. symbol_count={len(set(underlying_symbol_df.symbol.values))}. file_name={file_name}"
        )


def post_process_date(s3_fs_client, d: date) -> None:
    root_path = "finx-option-data-normalized/1min"
    folder = f"finx-option-data/{d.year}/{d.month}/{d.day}/"
    process_folder(s3_fs_client=s3_fs_client, folder=folder, root_path=root_path)


def data_is_unprocessed_for(s3_boto3_client, d: date) -> bool:
    raw_data_key = f"finx-option-data/{d.year}/{d.month}/{d.day}"
    finished_data_key = f"finx-option-data-normalized/1min/{d.year}/{d.month}/{d.day}/"

    raw_data = list_folders(
        s3_boto3_client, bucket_name=BUCKET_NAME, prefix=raw_data_key
    )
    if len(raw_data) == 0:
        # no data to process
        return False

    finished_data = list_folders(
        s3_boto3_client, bucket_name=BUCKET_NAME, prefix=finished_data_key
    )
    if len(finished_data) > 0:
        # finished data already there
        return False

    logger.debug(f"date={d} is unprocessed")
    return True


def handler_post_process_today(event, context):
    s3_boto3_client = boto3.client("s3")
    s3_fs_client = s3fs.S3FileSystem()

    d = date.today()

    if data_is_unprocessed_for(s3_boto3_client, d=d):
        post_process_date(s3_fs_client, d=d)


def handler_post_process_yesterday(event, context):
    s3_boto3_client = boto3.client("s3")
    s3_fs_client = s3fs.S3FileSystem()

    d = date.today() - timedelta(days=1)

    if data_is_unprocessed_for(s3_boto3_client, d=d):
        post_process_date(s3_fs_client, d=d)


def handler_post_process_last_x_days(x: int = 30):
    s3_boto3_client = boto3.client("s3")
    s3_fs_client = s3fs.S3FileSystem()

    today = date.today()

    for i in range(0, x):
        d = today - timedelta(days=i)
        print(f"d = {d}")
        if data_is_unprocessed_for(s3_boto3_client, d=d):
            post_process_date(s3_fs_client, d=d)


def handler_post_process_date(d: date):
    s3_boto3_client = boto3.client("s3")
    s3_fs_client = s3fs.S3FileSystem()

    if data_is_unprocessed_for(s3_boto3_client, d=d):
        post_process_date(s3_fs_client, d=d)
