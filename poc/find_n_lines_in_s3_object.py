# -*- coding: utf-8 -*-

"""
The best practice to find number of lines in text s3 object.
"""

from icecream import ic
import smart_open
from s3splitmerge.tests.data import create_s3_csv_file
from s3splitmerge.tests.aws import boto_ses, s3_client, bucket
from s3splitmerge.helpers import is_s3_object_exists

key = "s3splitmerge/poc/find_n_lines_in_s3_object/data.csv"
uri = f"s3://{bucket}/{key}"
if not is_s3_object_exists(s3_client, bucket, key):
    create_s3_csv_file(
        boto_ses,
        n_k_rows=1,
        header=True,
        bucket=bucket,
        key=key,
    )

with smart_open.open(uri, "r") as obj:
    for id, line in enumerate(obj):
        pass

print(f"{uri} has {id + 1} lines")


with smart_open.open(uri, "r") as obj:
    print(type(obj))
    ic(obj.readlines())