# -*- coding: utf-8 -*-

from s3splitmerge.tests.aws import boto_ses, bucket, prefix
from s3splitmerge.lbd.hdl_split_csv import lbd_func_split_csv

res = lbd_func_split_csv.handler(
    event=dict(
        source_bucket=bucket,
        source_key=f"{prefix}/big-file/csv-1MB.csv",
        target_bucket=bucket,
        target_key=f"{prefix}/speed-test/split/csv/by_size/csv-1MB/{{i}}.csv",
        header=True,
        target_size=1024 * 300,
    ),
    context=None,
)

res = lbd_func_split_csv.handler(
    event=dict(
        source_bucket=bucket,
        source_key=f"{prefix}/big-file/csv-1MB.csv",
        target_bucket=bucket,
        target_key=f"{prefix}/speed-test/split/csv/by_rows/csv-1MB/{{i}}.csv",
        header=True,
        target_rows=3000,
    ),
    context=None,
)
