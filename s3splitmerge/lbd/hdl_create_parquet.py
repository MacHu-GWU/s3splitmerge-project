# -*- coding: utf-8 -*-

import attr
import random
import pandas as pd
import awswrangler as wr
from datetime import datetime, timedelta

from .func import Event, LambdaFunction
from ..boto_ses import boto_ses

s3_client = boto_ses.client("s3")

hexdigits = "0123456789abcdef"


def rand_str(length):
    return "".join([random.choice(hexdigits) for _ in range(length)])


@attr.s
class CreateParquetEvent(Event):
    bucket: str = attr.ib()
    key: str = attr.ib()
    lower_id: int = attr.ib()
    upper_id: int = attr.ib()
    date: str = attr.ib()


class LbdFuncCreateParquet(LambdaFunction):
    name = "handler_create_parquet"

    def handler(self, event, context):
        evt = CreateParquetEvent(**event)
        df = pd.DataFrame(columns=["id", "time", "text"])
        n = evt.upper_id - evt.lower_id + 1
        df["id"] = range(evt.lower_id, evt.upper_id + 1)
        start_time = datetime.strptime(evt.date, "%Y-%m-%d")
        end_time = start_time + timedelta(days=1) - timedelta(seconds=1)
        df["time"] = pd.date_range(start_time, end_time, periods=n)
        df["text"] = [rand_str(32) for _ in range(n)]
        wr.s3.to_parquet(
            df=df,
            path=f"s3://{evt.bucket}/{evt.key}",
        )


lbd_func_create_parquet = LbdFuncCreateParquet()
