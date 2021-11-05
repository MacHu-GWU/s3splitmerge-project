# -*- coding: utf-8 -*-

import attr
import typing
from attrs_mate import AttrsClass


#--- SQS
@attr.s
class SQSRecord(AttrsClass):
    messageId: str = attr.ib()
    receiptHandle: str = attr.ib()
    body: str = attr.ib()
    attributes: dict = attr.ib()
    messageAttributes: dict = attr.ib()
    md5OfBody: str = attr.ib()
    eventSource: str = attr.ib()
    eventSourceARN: str = attr.ib()
    awsRegion: str = attr.ib()


@attr.s
class SQSEvent(AttrsClass):
    Records: typing.List[SQSRecord] = SQSRecord.ib_list_of_nested()


# --- S3Put
@attr.s
class Bucket(AttrsClass):
    name: str = attr.ib()
    ownerIdentity: dict = attr.ib()
    arn: str = attr.ib()


@attr.s
class Object(AttrsClass):
    key: str = attr.ib()
    size: int = attr.ib()
    eTag: str = attr.ib()
    sequencer: str = attr.ib()


@attr.s
class S3(AttrsClass):
    s3SchemaVersion: str = attr.ib()
    configurationId: str = attr.ib()
    bucket: Bucket = Bucket.ib_nested()
    object: Object = Object.ib_nested()


@attr.s
class S3PutEvent(AttrsClass):
    eventVersion: str = attr.ib()
    eventSource: str = attr.ib()
    awsRegion: str = attr.ib()
    eventTime: str = attr.ib()
    eventName: str = attr.ib()
    userIdentity: dict = attr.ib()
    requestParameters: dict = attr.ib()
    responseElements: dict = attr.ib()
    s3: S3 = S3.ib_nested()
#
# {
#     "Records": [
#         {
#             "eventVersion": "2.0",
#             "eventSource": "aws:s3",
#             "awsRegion": "us-east-1",
#             "eventTime": "1970-01-01T00:00:00.000Z",
#             "eventName": "ObjectCreated:Put",
#             "userIdentity": {
#                 "principalId": "EXAMPLE"
#             },
#             "requestParameters": {
#                 "sourceIPAddress": "127.0.0.1"
#             },
#             "responseElements": {
#                 "x-amz-request-id": "EXAMPLE123456789",
#                 "x-amz-id-2": "EXAMPLE123/5678abcdefghijklambdaisawesome/mnopqrstuvwxyzABCDEFGH"
#             },
#             "s3": {
#                 "s3SchemaVersion": "1.0",
#                 "configurationId": "testConfigRule",
#                 "bucket": {
#                     "name": "example-bucket",
#                     "ownerIdentity": {
#                         "principalId": "EXAMPLE"
#                     },
#                     "arn": "arn:aws:s3:::example-bucket"
#                 },
#                 "object": {
#                     "key": "test%2Fkey",
#                     "size": 1024,
#                     "eTag": "0123456789abcdef0123456789abcdef",
#                     "sequencer": "0A1B2C3D4E5F678901"
#                 }
#             }
#         }
#     ]
# }
#
#
# @attr.s
# class SplitEvent(Base):
#     source_bucket: str = attr.ib()
#     source_key: str = attr.ib()
#     target_bucket: str = attr.ib()
#     target_key: str = attr.ib()
#     header: bool = attr.ib()
#     target_size: int = attr.ib(default=None)
#     target_rows: int = attr.ib(default=None)
#
#
# @attr.s
# class MergeEvent(Base):
#     source_bucket: str = attr.ib()
#     source_key: str = attr.ib()
#     target_bucket: str = attr.ib()
#     target_key: str = attr.ib()
#     target_size: int = attr.ib()
