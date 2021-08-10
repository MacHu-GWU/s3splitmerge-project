# -*- coding: utf-8 -*-

import boto3
from dataclasses import dataclass
from s3splitmerge.split import split_csv_by_size, split_csv_by_rows
from s3splitmerge.merge import merge_parquet_by_prefix
from chalice import Chalice

boto_ses = boto3.session.Session()
s3_client = boto_ses.client("s3")

app = Chalice(app_name="s3splitmerge")


@dataclass
class SplitEvent:
    source_bucket: str
    source_key: str
    target_bucket: str
    target_key: str
    target_size: int = None
    target_rows: int = None


@app.lambda_function(name="handler_split_csv")
def handler_split_csv(event, context):
    """
    Profile Records:

    Case 1, csv 1GB

    - 7,500,000 lines
    - each line is: ``id,value``
    - id = 1 ~ 7,500,000
    - value = "a" * 128
    - split into 20 * 50MB file

    Performance:

    - by size:
        - 116 secs
        - max memory used: 134 MB
        - event::

            {
                "source_bucket": "aws-data-lab-sanhe-aws-etl-solutions",
                "source_key": "s3splitmerge/tests/big-file/csv-1GB.csv",
                "target_bucket": "aws-data-lab-sanhe-aws-etl-solutions",
                "target_key": "s3splitmerge/tests/big-file/csv-1GB-{i}.csv",
                "target_size": 50 * 1024 * 1024
            }
    - by rows:
        - 71 secs
        - max memory used: 134 MB
        - event::

            {
                "source_bucket": "aws-data-lab-sanhe-aws-etl-solutions",
                "source_key": "s3splitmerge/tests/big-file/csv-1GB.csv",
                "target_bucket": "aws-data-lab-sanhe-aws-etl-solutions",
                "target_key": "s3splitmerge/tests/big-file/csv-1GB-{i}.csv",
                "target_rows": 375 * 1000
            }

    """
    se = SplitEvent(**event)
    if se.target_size is not None:
        return split_csv_by_size(
            s3_client=s3_client,
            source_bucket=se.source_bucket,
            source_key=se.source_key,
            target_bucket=se.target_bucket,
            target_key=se.target_bucket,
            target_size=se.target_size,
            header=True,
        )
    elif se.target_rows is not None:
        return split_csv_by_rows(
            s3_client=s3_client,
            source_bucket=se.source_bucket,
            source_key=se.source_key,
            target_bucket=se.target_bucket,
            target_key=se.target_bucket,
            target_rows=se.target_rows,
            header=True,
        )
    else:
        return {"error": {"event": event}}


@dataclass
class MergeEvent:
    source_bucket: str
    source_key: str
    target_bucket: str
    target_key: str
    target_size: int


@app.lambda_function(name="handler_merge_parquet")
def handler_merge_parquet(event, context):
    """
    {
        "source_bucket": "aws-data-lab-sanhe-aws-etl-solutions",
        "source_key": "s3splitmerge/tests/many-file/parquet-1GB/",
        "target_bucket": "aws-data-lab-sanhe-aws-etl-solutions",
        "target_key": "s3splitmerge/tests/out/many-file/parquet-1GB/{i}.parquet",
        "target_size": 250 * 1024 * 1024
    }


    {
        "source_bucket": "aws-data-lab-sanhe-aws-etl-solutions",
        "source_key": "s3splitmerge/tests/many-file/parquet-1MB/",
        "target_bucket": "aws-data-lab-sanhe-aws-etl-solutions",
        "target_key": "s3splitmerge/tests/out/many-file/parquet-1MB/{i}.parquet",
        "target_size": 500 * 1024
    }
    """
    me = MergeEvent(**event)
    return merge_parquet_by_prefix(
        boto3_session=boto_ses,
        source_bucket=me.source_bucket,
        source_key_prefix=me.source_key,
        target_bucket=me.target_bucket,
        target_key=me.target_key,
        target_size=me.target_size,
    )
