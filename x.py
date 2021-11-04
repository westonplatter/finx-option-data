import boto3
from dotenv import dotenv_values
import pyarrow.parquet as pq
import s3fs
import pandas as pd
from tqdm import tqdm
from typing import List


configs = dotenv_values(".env")


# constants - data
BUCKET_NAME: str = configs["BUCKET_NAME"]

# semi constants
raw_prefix = "finx-option-data/2021/10/"
finished_prefix = "finx-option-data-finished/2021/10/"

# global variables
s3_boto3_client = boto3.client("s3")
s3_fs_client = s3 = s3fs.S3FileSystem()


def list_folders(s3_client, bucket_name, prefix):
    result = s3_client.list_objects(Bucket=bucket_name, Prefix=prefix, Delimiter="/")
    prefixes = result.get("CommonPrefixes")
    if prefixes is None:
        return []
    folders = [o.get("Prefix") for o in prefixes]
    return folders


def get_unprocessed_folders(
    s3_client, raw_prefix: str, finished_prefix: str
) -> List[str]:
    raw_data_folders = list_folders(s3_client, BUCKET_NAME, raw_prefix)
    finished_data_folders = list_folders(s3_client, BUCKET_NAME, finished_prefix)
    unprocessed_folders = list(set(raw_data_folders) - set(finished_data_folders))
    return unprocessed_folders


def fetch_df(s3_fs_client, folder: str) -> pd.DataFrame:
    s3_folder_url = (f"s3://{BUCKET_NAME}/{folder}")[:-1]
    df = (
        pq.ParquetDataset(s3_folder_url, filesystem=s3_fs_client)
        .read_pandas()
        .to_pandas()
    )
    return df


def transform_df(df) -> pd.DataFrame:
    """Note - we mutate the in memory dataframe and return it"""

    # add underlying symbol
    df["underlying_symbol"] = df.symbol.str.split("_").str[0]

    # convert trade/quote time to Eastern Time
    df["trade_time"] = pd.to_datetime(df["tradeTimeInLong"], unit="ms").dt.tz_localize(
        tz="US/Eastern"
    )
    df["quote_time"] = pd.to_datetime(df["quoteTimeInLong"], unit="ms").dt.tz_localize(
        tz="US/Eastern"
    )

    # set index on quote time
    df.set_index("quote_time", drop=False, inplace=True)

    return df


unprocessed_folders = get_unprocessed_folders(
    s3_client=s3_boto3_client, raw_prefix=raw_prefix, finished_prefix=finished_prefix
)


# folder = unprocessed_folders[1]
folder: str = "finx-option-data/2021/10/29/"
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
    "daysToExpiration",
    "theoreticalVolatility",
    "theoreticalOptionValue",
    "underlying_symbol",
    "symbol",
]
agg_dict = {}
for c in columns_to_aggregate:
    agg_dict[c] = "last"


ddfs: List[pd.DataFrame] = []

for underlying_symbol in list(set(df.underlying_symbol.values)):
    symbol_df = df.query('underlying_symbol == @underlying_symbol')

    values = list(set(symbol_df.symbol.values))

    print(f"{underlying_symbol}. symbol count = {len(values)}")
    
    for symbol in tqdm(values):
        # TODO(weston) agg is super slow in the way I'm using it here.
        (symbol_df.query("symbol == @symbol")
            .resample("1min")
            .agg(agg_dict))
        # print(f"Finished {symbol}. Count = {df.query('symbol == @symbol').count()['quote_time']}")
        # ddfs.append(ddf)
