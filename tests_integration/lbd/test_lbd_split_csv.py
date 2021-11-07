# -*- coding: utf-8 -*-

import json
from rich import print
from s3splitmerge.tests.aws import boto_ses, bucket, prefix
from s3splitmerge.lbd.hdl_split_csv import lbd_func_split_csv

lbd_client = boto_ses.client("lambda")

# Duration: 568.50 ms	Billed Duration: 569 ms	Memory Size: 1024 MB	Max Memory Used: 250 MB
event_split_csv_1MB = dict(
    source_bucket=bucket,
    source_key=f"{prefix}/big-file/csv-1MB.csv",
    target_bucket=bucket,
    target_key=f"{prefix}/speed-test/split/csv/by_size/csv-1MB/{{i}}.csv",
    header=True,
    target_size=300 * 1024,  # 300kb
)

# Duration: 6508.43 ms	Billed Duration: 6509 ms	Memory Size: 1024 MB	Max Memory Used: 250 MB
event_split_csv_100MB = dict(
    source_bucket=bucket,
    source_key=f"{prefix}/big-file/csv-100MB.csv",
    target_bucket=bucket,
    target_key=f"{prefix}/speed-test/split/csv/by_size/csv-100MB/{{i}}.csv",
    header=True,
    target_size=30 * 1024 ** 2,  # 30MB
)

# Duration: 20972.70 ms	Billed Duration: 20973 ms	Memory Size: 1024 MB	Max Memory Used: 363 MB
event_split_csv_300MB = dict(
    source_bucket=bucket,
    source_key=f"{prefix}/big-file/csv-300MB.csv",
    target_bucket=bucket,
    target_key=f"{prefix}/speed-test/split/csv/by_size/csv-300MB/{{i}}.csv",
    header=True,
    target_size=100 * 1024 ** 2,  # 100 MB
)

# target_size = 300MB Duration: 59726.31 ms	Billed Duration: 59727 ms	Memory Size: 1024 MB	Max Memory Used: 564 MB
# target_size = 50MB Duration: 62775.87 ms	Billed Duration: 62776 ms	Memory Size: 1024 MB	Max Memory Used: 564 MB
event_split_csv_1GB = dict(
    source_bucket=bucket,
    source_key=f"{prefix}/big-file/csv-1GB.csv",
    target_bucket=bucket,
    target_key=f"{prefix}/speed-test/split/csv/by_size/csv-1GB/{{i}}.csv",
    header=True,
    target_size=50 * 1024 ** 2,  # 300 MB
)

# ------
event = event_split_csv_1GB
print(json.dumps(event))

# run lambda handler locally
# res = lbd_func_split_csv.handler(event=event, context=None)
# print(res)

# run lambda function on aws
# res = lbd_client.invoke(
#     FunctionName=f"s3splitmerge-dev-{lbd_func_split_csv.name}",
#     Payload=json.dumps(event).encode("utf-8"),
# )
# print(res["Payload"].read())
