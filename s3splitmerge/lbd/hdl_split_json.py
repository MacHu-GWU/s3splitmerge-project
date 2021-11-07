# -*- coding: utf-8 -*-

import enum
import attr
from attrs_mate import AttrsClass
from .func import LambdaFunction
from ..split import split_csv_by_size, split_csv_by_rows
from .event import SQSEvent, S3PutEvent
from ..boto_ses import boto_ses

s3_client = boto_ses.client("s3")


class Case(enum.Flag):
    request = enum.auto()
    sqs = enum.auto()
    s3put = enum.auto()


@attr.s
class SplitCsvEvent(AttrsClass):
    source_bucket: str = attr.ib()
    source_key: str = attr.ib()
    target_bucket: str = attr.ib()
    target_key: str = attr.ib()
    header: bool = attr.ib()
    target_size: int = attr.ib(default=None)
    target_rows: int = attr.ib(default=None)


class LbdFuncSplitCsv(LambdaFunction):
    name = "handler_split_csv"

    def handler(self, event, context):
        case = None
        if "Records" in event:
            if len(event["Records"]):
                record = event["Records"][0]
                if "s3" in record:
                    case = Case.s3put
                elif "messageId" in record:
                    case = Case.sqs
                else:
                    pass
        elif "source_bucket" in event and "source_key" in event:
            case = Case.request

        if case is None:
            raise Exception
        elif case is Case.request:
            evt = SplitCsvEvent(**event)
            kwargs = evt.to_dict()
            kwargs["s3_client"] = s3_client
            if bool(evt.target_size) and bool(evt.target_rows):
                raise ValueError
            elif evt.target_size is not None:
                del kwargs["target_rows"]
                return split_csv_by_size(**kwargs)
            elif evt.target_rows is not None:
                del kwargs["target_size"]
                return split_csv_by_rows(**kwargs)
            else:
                raise ValueError
        elif case is Case.sqs:
            evt = SQSEvent(**event)
            return {}
        elif case is Case.s3put:
            evt = S3PutEvent(**event)
            return {}
        else:
            raise Exception


lbd_func_split_csv = LbdFuncSplitCsv()
