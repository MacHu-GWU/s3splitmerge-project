# -*- coding: utf-8 -*-

"""
In general, split by rows is 2x faster than split by size. Because we don't need
to track number size written to buffer.
"""

import io
import math
import typing

import smart_open
from .helpers import check_enumeration_s3_key_string, get_s3_object_metadata
from .options import ZFILL


def split_csv_by_size(s3_client,
                      source_bucket: str,
                      source_key: str,
                      target_bucket: str,
                      target_key: str,
                      target_size: int,
                      header: bool,
                      zfill: int = ZFILL) -> typing.List[typing.Tuple[str, str]]:
    """
    .. note::

        **Doesn't support compressed csv file**
    """
    check_enumeration_s3_key_string(target_key)

    target_s3_bucket_key_list = list()

    nth_file = 0
    with smart_open.open(
            f"s3://{source_bucket}/{source_key}", "rb",
            transport_params=dict(client=s3_client)
    ) as s3obj:
        buffer = io.BytesIO()
        buffer_size = 0
        if header:
            header_line = s3obj.readline()
            buffer.write(header_line)

        for line in s3obj:
            buffer_size += buffer.write(line)
            if buffer_size >= target_size:
                nth_file += 1
                s3_client.put_object(
                    Bucket=target_bucket,
                    Key=target_key.format(i=str(nth_file).zfill(zfill)),
                    Body=buffer.getvalue()
                )
                target_s3_bucket_key_list.append(
                    (
                        target_bucket,
                        target_key.format(i=str(nth_file).zfill(zfill))
                    )
                )
                buffer = io.BytesIO()
                buffer_size = 0
                if header:
                    buffer.write(header_line)

        if buffer_size:
            nth_file += 1
            s3_client.put_object(
                Bucket=target_bucket,
                Key=target_key.format(i=str(nth_file).zfill(zfill)),
                Body=buffer.getvalue()
            )
            target_s3_bucket_key_list.append(
                (
                    target_bucket,
                    target_key.format(i=str(nth_file).zfill(zfill))
                )
            )

    return target_s3_bucket_key_list


def split_csv_by_rows(s3_client,
                      source_bucket: str,
                      source_key: str,
                      target_bucket: str,
                      target_key: str,
                      target_rows: int,
                      header: bool,
                      zfill: int = ZFILL) -> typing.List[typing.Tuple[str, str]]:
    """
    - Small memory usage
    - 2 x faster than v1
    """
    check_enumeration_s3_key_string(target_key)

    target_s3_bucket_key_list = list()

    with smart_open.open(
            f"s3://{source_bucket}/{source_key}", "r",
            transport_params=dict(client=s3_client)
    ) as f_in:
        if header:
            header_line = f_in.readline()

        nth_file = 0
        while 1:
            nth_file += 1
            one_more_line = f_in.readline()
            if one_more_line:
                with smart_open.open(
                        f"s3://{target_bucket}/{target_key.format(i=str(nth_file).zfill(zfill))}", "w",
                        transport_params=dict(client=s3_client),
                ) as f_out:
                    if header:
                        f_out.write(header_line)
                    f_out.write(one_more_line)
                    for _ in range(target_rows - 1):
                        f_out.write(f_in.readline())

                    target_s3_bucket_key_list.append(
                        (
                            target_bucket,
                            target_key.format(i=str(nth_file).zfill(zfill))
                        )
                    )
            else:
                break
    return target_s3_bucket_key_list


def split_json_by_size(s3_client,
                       source_bucket: str,
                       source_key: str,
                       target_bucket: str,
                       target_key: str,
                       target_size: int,
                       zfill: int = ZFILL):
    """
    """
    return split_csv_by_size(
        s3_client=s3_client,
        source_bucket=source_bucket,
        source_key=source_key,
        target_bucket=target_bucket,
        target_key=target_key,
        target_size=target_size,
        header=False,
        zfill=zfill,
    )


def split_json_by_rows(s3_client,
                       source_bucket: str,
                       source_key: str,
                       target_bucket: str,
                       target_key: str,
                       target_rows: int,
                       zfill: int = ZFILL) -> typing.List[typing.Tuple[str, str]]:
    """
    - Small memory usage
    - 2 x faster than v1
    """
    return split_csv_by_rows(
        s3_client=s3_client,
        source_bucket=source_bucket,
        source_key=source_key,
        target_bucket=target_bucket,
        target_key=target_key,
        target_rows=target_rows,
        header=False,
        zfill=zfill,
    )
