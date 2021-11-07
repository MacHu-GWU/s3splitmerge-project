# -*- coding: utf-8 -*-

import json
from rich import print
from s3splitmerge.tests.aws import boto_ses, bucket, prefix
from s3splitmerge.lbd.hdl_split_parquet import lbd_func_split_parquet

lbd_client = boto_ses.client("lambda")

res = lbd_func_split_parquet.handler(event={}, context=None)
# print(res)

# run lambda function on aws
# res = lbd_client.invoke(
#     FunctionName=f"s3splitmerge-dev-{lbd_func_split_csv.name}",
#     Payload=json.dumps(event).encode("utf-8"),
# )
# print(res["Payload"].read())

# REPORT RequestId: c5464c3f-fcd8-492b-8c33-789f1ee98118	Duration: 57093.18 ms	Billed Duration: 57094 ms	Memory Size: 1024 MB	Max Memory Used: 362 MB	Init Duration: 4029.54 ms
# REPORT RequestId: 8a2f96b9-d681-444e-80ef-9d9e1db574bf	Duration: 5860.78 ms	Billed Duration: 5861 ms	Memory Size: 1024 MB	Max Memory Used: 362 MB
