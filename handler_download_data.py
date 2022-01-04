from dotenv import dotenv_values
from loguru import logger
import s3fs


configs = dotenv_values(".env")


# constants - data
BUCKET_NAME: str = configs["BUCKET_NAME"]


def download_parquet_file(year_month_day: str):
    fs = s3fs.S3FileSystem()

    year, month, day = year_month_day.split("-")
    root_path = f"finx-option-data-normalized/1min/{year}/{month}/{day}/"
    key = f"s3://{BUCKET_NAME}/{root_path}"

    parquet_files = fs.glob(f"{key}/*")

    for file in parquet_files:
        new_file_name = file.replace(
            f"{BUCKET_NAME}/finx-option-data-normalized/", "data/normalized/"
        )
        fs.download(rpath=file, lpath=new_file_name, recursive=True)
        logger.info(f"Downloaded folder -- {file}")


if __name__ == "__main__":
    download_parquet_file("2021-11-23")
