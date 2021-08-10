# -*- coding: utf-8 -*-

import typing
import smart_open
from .exc import S3ObjectNotFound, InvalidEnumerationS3Key


class Metadata:
    def __init__(self, size: int, etag: str):
        self.size = size
        self.etag = etag


def get_s3_object_metadata(s3_client, bucket, key) -> Metadata:
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
        content_length = response["ContentLength"]
        etag = response["ETag"]
        return Metadata(size=content_length, etag=etag)
    except Exception as e:
        if "Not Found" in str(e):
            raise S3ObjectNotFound


def is_s3_object_exists(s3_client, bucket, key) -> bool:
    """
    Test if a s3 object exists.
    """
    try:
        get_s3_object_metadata(s3_client, bucket, key)
        return True
    except S3ObjectNotFound:
        return False
    except Exception:
        raise


def count_lines_in_s3_object(s3_client, bucket, key) -> int:
    """
    Find number of lines in a text s3 object.
    """
    with smart_open.open(f"s3://{bucket}/{key}", "r", transport_params=dict(client=s3_client)) as obj:
        for id, line in enumerate(obj):
            pass
    return id + 1


def check_enumeration_s3_key_string(s3_key: str):
    if "{i}" not in s3_key:
        raise InvalidEnumerationS3Key


def get_key_size_all_objects(s3_client,
                             bucket: str,
                             prefix: str,
                             max_items: int = 1000000) -> typing.List[typing.Tuple[str, int]]:
    """
    Given a S3 Bucket and Prefix, returns the s3 key and file size in bytes
    for all s3 objects under the prefix. The boto3 API can only return 1000
    records per request, this function automatically handles the pagination
    and returns all results. But you should not abuse it if there's truely
    too many records.
    """
    n_objs_per_request = 1000
    key_and_size_list = list()
    next_continuation_token = None
    n_items = 0
    while 1:
        kwargs = dict(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=n_objs_per_request,
        )
        if next_continuation_token is not None:
            kwargs["ContinuationToken"] = next_continuation_token
        response = s3_client.list_objects_v2(**kwargs)

        for dct in response["Contents"]:
            key = dct["Key"]
            size = dct["Size"]
            key_and_size_list.append((key, size))

        is_truncated = response["IsTruncated"]
        n_keys = response["KeyCount"]
        n_items += n_keys

        if n_items >= max_items:
            break

        if is_truncated:
            next_continuation_token = response["NextContinuationToken"]
        else:
            break

    return key_and_size_list


def group_s3_objects_no_larger_than(key_and_size_list: typing.List[typing.Tuple[str, int]],
                                    max_size: int) -> typing.List[typing.List[str]]:
    """
    Given lots of s3 objects key and their size in bytes, with a max size
    limitation, group them into different group that the total file size of
    each group is as large as possible but not pass max_size settings.

    Return a list of s3 key group, each s3 key group is just a list of string
    (list of s3 key)
    """
    group_list: typing.List[typing.List[str]] = list()
    s3_object_group: typing.List[str] = [key_and_size_list[0][0], ]
    cumulated_size = key_and_size_list[0][1]
    for key, size in key_and_size_list[1:]:
        # if add this one will surpass the max_size
        if cumulated_size + size < max_size:
            s3_object_group.append(key)
            cumulated_size += size
        else:
            group_list.append(s3_object_group)
            s3_object_group = [key, ]
            cumulated_size = size

    if len(s3_object_group):
        group_list.append(s3_object_group)

    return group_list
