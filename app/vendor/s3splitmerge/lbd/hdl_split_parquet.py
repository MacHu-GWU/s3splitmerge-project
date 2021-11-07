# -*- coding: utf-8 -*-

import enum
import s3fs
from fastparquet import ParquetFile
import attr
from attrs_mate import AttrsClass
import awswrangler as wr
from .func import LambdaFunction
from ..split import split_csv_by_size, split_csv_by_rows
from .event import SQSEvent, S3PutEvent
from ..boto_ses import boto_ses

s3_client = boto_ses.client("s3")


# class Case(enum.Flag):
#     request = enum.auto()
#     sqs = enum.auto()
#     s3put = enum.auto()
#
#
# @attr.s
# class SplitCsvEvent(AttrsClass):
#     source_bucket: str = attr.ib()
#     source_key: str = attr.ib()
#     target_bucket: str = attr.ib()
#     target_key: str = attr.ib()
#     header: bool = attr.ib()
#     target_size: int = attr.ib(default=None)
#     target_rows: int = attr.ib(default=None)

class LbdFuncSplitParquet(LambdaFunction):
    name = "handler_split_parquet"

    def handler(self, event, context):
        from s3splitmerge.tests.aws import bucket, prefix
        key = f"{prefix}/poc/split_parquet_before.parquet"
        s3 = s3fs.S3FileSystem()
        myopen = s3.open
        pf = ParquetFile(f"/{bucket}/{key}", open_with=myopen)
        for ind, df in enumerate(pf.iter_row_groups()):
            print(df.shape)
            wr.s3.to_parquet(
                df=df,
                path=f"s3://{bucket}/{key}".replace(".parquet", f"{ind + 1}.parquet")
            )

lbd_func_split_parquet = LbdFuncSplitParquet()
