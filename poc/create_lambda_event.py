# -*- coding: utf-8 -*-

import json
import base64
from rich import print
from s3splitmerge.tests.aws import boto_ses
from s3splitmerge.tests.lbd import event_maker
from s3splitmerge.lbd.func import Funcs

lbd_client = boto_ses.client("lambda")

res = lbd_client.invoke(
    FunctionName=f"s3splitmerge-dev-{Funcs.lbd_func_split_csv}",
    InvocationType="RequestResponse",
    LogType="Tail",
    Payload=json.dumps(event_maker.split_csv_by_size()),
)
print(base64.b64decode(res["LogResult"].encode("utf-8")).decode("utf-8"))