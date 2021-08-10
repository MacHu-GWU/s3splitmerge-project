# -*- coding: utf-8 -*-

import io
import time
import boto3
from boto3.s3.transfer import TransferConfig
from icecream import ic
import awswrangler as wr
from datetime import datetime
import pandas as pd
from pathlib_mate import Path

boto_ses = boto3.session.Session()
s3_client = boto_ses.client("s3")
bucket = "aws-data-lab-sanhe-aws-etl-solutions"


def create_big_pure_text_file():
    n_lines = 1000
    value = "alice@example.com"
    f = Path(__file__).change(new_basename="big-pure-text-file.csv")
    if not f.exists():
        with open(f.abspath, "a") as f:
            f.write("id,value\n")
            for id in range(n_lines):
                f.write(f"{id},{value}\n")


def create_big_csv_file_with_pandas():
    # 1000, 1000 = 25MB
    n_lines_per_df = 1000
    n_df = 1000
    columns = ["id", "value"]
    value = "alice@example.com"
    f = Path(__file__).change(new_basename="big-csv-file.csv")
    if not f.exists():
        for nth_df in range(n_df):
            data = list()
            for nth_line in range(nth_df * n_lines_per_df, (nth_df + 1) * n_lines_per_df):
                data.append((nth_line, value))
            df = pd.DataFrame(data, columns=columns)
            if f.exists():
                header = False
            else:
                header = True
            df.to_csv(
                f.abspath,
                index=False,
                header=header,
                mode="a",
            )


def create_big_json_file_with_pandas():
    # 1000, 1000 = 41MB
    n_lines_per_df = 5000
    n_df = 1000
    columns = ["id", "value"]
    value = "alice@example.com"
    f = Path(__file__).change(new_basename="big-json-file.json")
    if not f.exists():
        for nth_df in range(n_df):
            data = list()
            for nth_line in range(nth_df * n_lines_per_df, (nth_df + 1) * n_lines_per_df):
                data.append((nth_line, value))
            df = pd.DataFrame(data, columns=columns)
            buffer = io.StringIO()
            df.to_json(
                buffer,
                orient="records",
                lines=True,
            )
            with open(f.abspath, "a") as file:
                file.write(buffer.getvalue())


def s3_regular_upload():
    """
    Regular, single concurrent upload.
    """
    MB = 1024 ** 2
    config = TransferConfig(multipart_threshold=1000 * MB)
    f = Path(__file__).change(new_basename="big-json-file.json")

    key = "playground/multipart/big-json-file1.json"
    before_time = datetime.utcnow()
    res = s3_client.upload_file(f.abspath, Bucket=bucket, Key=key, Config=config)
    after_time = datetime.utcnow()
    ic(res)
    elapse = (after_time - before_time).total_seconds()
    ic(elapse) # 450 MB = 123 seconds


def s3_multi_upload():
    """
    Multi part, multi concurrent upload
    """
    # default is 8MB threshold
    MB = 1024 ** 2
    config = TransferConfig(multipart_threshold=8 * MB, io_chunksize=1 * MB)

    f = Path(__file__).change(new_basename="big-json-file.json")

    key = "playground/multipart/big-json-file2.json"
    before_time = datetime.utcnow()
    res = s3_client.upload_file(f.abspath, Bucket=bucket, Key=key, Config=config)
    after_time = datetime.utcnow()
    ic(res)
    elapse = (after_time - before_time).total_seconds()
    ic(elapse) # 450 MB = 123 seconds


if __name__ == "__main__":
    # create_big_pure_text_file()
    # create_big_csv_file_with_pandas()
    # create_big_json_file_with_pandas()
    s3_regular_upload()
    s3_multi_upload()
    pass
