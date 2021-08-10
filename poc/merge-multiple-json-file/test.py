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


class Config:
    bucket = "aws-data-lab-sanhe-aws-etl-solutions"
    key_prefix = "s3splitmerge/poc/merge-multiple-json-file"
    n_file = 3
    n_records_per_file = 150000

bucket = "aws-data-lab-sanhe-aws-etl-solutions"
key_prefix = "s3splitmerge/poc/merge-multiple-json-file"

def create_test_data():
    n_file = 3
    n_records_per_file = 150000
    columns = ["id", "value"]
    value = "alice@example.com"
    for nth_file in range(1, 1+n_file):
        start_id = (nth_file - 1) * n_records_per_file + 1
        end_id = start_id + n_records_per_file
        df = pd.DataFrame(columns=columns)
        df["id"] = range(start_id, end_id)
        df["value"] = value

        wr.s3.to_json(
            df=df,
            path=f"s3://{bucket}/{key_prefix}/{nth_file}.json",
            orient="records",
            lines=True,
        )

def merge_files():
    KB = 1024
    config = TransferConfig(multipart_threshold=1)

    target_key = f"{key_prefix}/data.json"
    response = s3_client.create_multipart_upload(
        Bucket=bucket,
        Key=target_key,
    )
    upload_id = response["UploadId"]

    n_file = 3
    s3_key_lst = [
        f"{key_prefix}/{nth_file}.json"
        for nth_file in range(1, 1+n_file)
    ]

    parts = list()
    for part_number, s3_key in enumerate(s3_key_lst):
        part_number += 1
        response = s3_client.upload_part_copy(
            Bucket=bucket,
            Key=target_key,
            CopySource={"Bucket": bucket, "Key": s3_key},
            PartNumber=part_number,
            UploadId=upload_id,
        )
        etag = response["CopyPartResult"]["ETag"]
        parts.append({"ETag": etag, "PartNumber": part_number})

    s3_client.complete_multipart_upload(
        Bucket=bucket,
        Key=target_key,
        MultipartUpload={"Parts": parts},
        UploadId=upload_id
    )


if __name__ == "__main__":
    create_test_data()
    merge_files()
    pass
