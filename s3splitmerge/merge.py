# -*- coding: utf-8 -*-

import typing
import pandas as pd
import smart_open
import awswrangler as wr
from .helpers import (
    check_enumeration_s3_key_string,
    get_key_size_all_objects,
    group_s3_objects_no_larger_than,
)
from .options import ZFILL


def merge_parquet(boto3_session,
                  source_uri_list: typing.List[str],
                  target_bucket: str,
                  target_key: str) -> typing.Tuple[str, str]:
    """
    Merge multiple parquet file on S3 into one parquet file.

    .. note::

        For parquet, it has to use the awswrangler API and it only support
        boto3_session other than s3_client.
    """
    df_list = list()
    for s3_uri in source_uri_list:
        df = wr.s3.read_parquet(s3_uri, boto3_session=boto3_session)
        df_list.append(df)

    df = pd.concat(df_list, axis=0)
    wr.s3.to_parquet(
        df=df,
        path=f"s3://{target_bucket}/{target_key}",
        boto3_session=boto3_session
    )
    return target_bucket, target_key


def merge_parquet_by_prefix(boto3_session,
                            source_bucket,
                            source_key_prefix,
                            target_bucket,
                            target_key,
                            target_size,
                            zfill: int = ZFILL) -> typing.List[typing.Tuple[str, str]]:
    """
    Smartly merge all parquet s3 object under the same prefix into one or many
    fixed size (approximately) parquet file.
    """
    check_enumeration_s3_key_string(target_key)

    s3_client = boto3_session.client("s3")
    target_s3_bucket_key_list = list()

    # analyze input data
    key_and_size_list = get_key_size_all_objects(
        s3_client=s3_client,
        bucket=source_bucket,
        prefix=source_key_prefix,
    )

    group_list = group_s3_objects_no_larger_than(
        key_and_size_list=key_and_size_list,
        max_size=target_size,
    )

    for nth_group, s3_object_group in enumerate(group_list):
        nth_group += 1
        source_uri_list = [
            f"s3://{source_bucket}/{s3_key}"
            for s3_key in s3_object_group
        ]
        bucket_and_key = merge_parquet(
            boto3_session=boto3_session,
            source_uri_list=source_uri_list,
            target_bucket=target_bucket,
            target_key=target_key.format(i=str(nth_group).zfill(zfill)),
        )
        target_s3_bucket_key_list.append(bucket_and_key)

    return target_s3_bucket_key_list


def merge_json(s3_client,
               source_uri_list: typing.List[str],
               target_bucket: str,
               target_key: str):
    transport_params = dict(client=s3_client)
    with smart_open.open(
            f"s3://{target_bucket}/{target_key}", "w",
            transport_params=transport_params,
    ) as f_out:
        for source_uri in source_uri_list:
            with smart_open.open(
                    source_uri, "r",
                    transport_params=transport_params,
            ) as f_in:
                for line in f_in:
                    f_out.write(line)


def merge_json_by_prefix(s3_client,
                         source_bucket: str,
                         source_key_prefix: str,
                         target_bucket: str,
                         target_key: str,
                         target_size: int,
                         zfill: int = ZFILL):
    check_enumeration_s3_key_string(target_key)

    # analyze input data
    key_and_size_list = get_key_size_all_objects(
        s3_client=s3_client,
        bucket=source_bucket,
        prefix=source_key_prefix,
    )

    group_list = group_s3_objects_no_larger_than(
        key_and_size_list=key_and_size_list,
        max_size=target_size,
    )

    for nth_group, s3_object_group in enumerate(group_list):
        nth_group += 1
        source_uri_list = [
            f"s3://{source_bucket}/{s3_key}"
            for s3_key in s3_object_group
        ]
        merge_json(
            s3_client=s3_client,
            source_uri_list=source_uri_list,
            target_bucket=target_bucket,
            target_key=target_key.format(i=str(nth_group).zfill(zfill)),
        )
