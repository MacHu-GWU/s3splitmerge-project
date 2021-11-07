# -*- coding: utf-8 -*-

import json
from rich import print
from s3splitmerge.tests.aws import boto_ses, bucket, prefix
from s3splitmerge.lbd.hdl_create_parquet import (
    CreateParquetEvent, lbd_func_create_parquet,
)

lbd_client = boto_ses.client("lambda")

event = CreateParquetEvent(
    bucket=bucket,
    key=f"{prefix}/poc/many-parquet/year=2000/month=1/day=1/1.parquet",
    lower_id=1,
    upper_id=50,
    date="2000-01-01",
)
print(event.to_json())

# --- run lambda handler locally
# res = lbd_func_create_parquet.handler(event=event.to_dict(), context=None)
# print(res)

# --- run lambda function on aws
# res = lbd_client.invoke(
#     FunctionName=f"s3splitmerge-dev-{lbd_func_create_parquet.name}",
#     Payload=event.to_json(),
# )
# print(res["Payload"].read())


from datetime import datetime, timedelta


class Config:
    start_date = datetime(2000, 1, 1)
    n_days = 100
    n_file_per_day = 10
    n_rows_per_file = 2400000


def create_datalake(config: Config):
    lower_id = 1 - config.n_rows_per_file
    for ith_day in range(1, config.n_days + 1):
        date = config.start_date + timedelta(days=(ith_day - 1))
        for ith_file in range(1, config.n_file_per_day + 1):
            key = f"{prefix}/datalake/many-parquet/year={date.year}/month={date.month}/day={date.day}/{ith_file}.parquet"
            print(key)
            lower_id += config.n_rows_per_file
            upper_id = lower_id + config.n_rows_per_file - 1
            event = CreateParquetEvent(
                bucket=bucket,
                key=key,
                lower_id=lower_id,
                upper_id=upper_id,
                date=str(date.date()),
            )
            res = lbd_client.invoke_async(
                FunctionName=f"s3splitmerge-dev-{lbd_func_create_parquet.name}",
                InvokeArgs=event.to_json(),
            )


# create_datalake(Config())
