# -*- coding: utf-8 -*-

import typing
import awswrangler as wr
from .data import (
    create_s3_csv_file,
    create_s3_json_file,
    create_many_parquet_file,
    create_many_json_file,
)
from ..merge import (
    merge_parquet_by_prefix,
    merge_json_by_prefix,
)
from ..helpers import (
    is_s3_object_exists,
)


def run_test_split_csv(boto_ses,
                       n_k_rows: int,
                       header: bool,
                       source_bucket: str,
                       source_key: str,
                       target_bucket: str,
                       target_key: str,
                       target_size_or_rows: int,
                       split_csv_func: typing.Callable,
                       force_redo: bool):
    """
    A parameterized split_csv_... function unit test executor.
    """
    s3_client = boto_ses.client("s3")
    # Create single source csv file if not exists
    if (force_redo) or (not is_s3_object_exists(s3_client, source_bucket, source_key)):
        create_s3_csv_file(
            boto_ses=boto_ses,
            n_k_rows=n_k_rows,
            header=header,
            bucket=source_bucket,
            key=source_key,
        )

    # If first target not exists, execute split csv
    first_target_key = target_key.format(i=1)
    if (force_redo) or (not is_s3_object_exists(s3_client, target_bucket, first_target_key)):
        split_csv_func(
            s3_client,
            source_bucket,
            source_key,
            target_bucket,
            target_key,
            target_size_or_rows,
            header,
        )

    # Verify small target csv files
    common_target_key_prefix = target_key.replace("{i}.csv", "")
    response = s3_client.list_objects(Bucket=target_bucket, Prefix=common_target_key_prefix)
    n_rows_total = 0
    previous_last_id = None
    if header:
        read_csv_additional_kwargs = {}
    else:
        read_csv_additional_kwargs = {"header": None}
    for nth_file, obj_meta in enumerate(response["Contents"]):
        nth_file += 1
        key = obj_meta["Key"]
        df = wr.s3.read_csv(
            path=f"s3://{target_bucket}/{key}",
            boto3_session=boto_ses,
            **read_csv_additional_kwargs
        )
        n_rows = df.shape[0]
        if header:
            first_id = df["id"].head(1).tolist()[0]
            last_id = df["id"].tail(1).tolist()[0]
        else:
            first_id = df[df.columns[0]].head(1).tolist()[0]
            last_id = df[df.columns[0]].tail(1).tolist()[0]
        n_rows_total += n_rows
        if nth_file != 1:
            assert previous_last_id + 1 == first_id
        previous_last_id = last_id
    assert n_rows_total == n_k_rows * 1000


def run_test_split_json(boto_ses,
                        n_k_rows: int,
                        source_bucket: str,
                        source_key: str,
                        target_bucket: str,
                        target_key: str,
                        target_size_or_rows: int,
                        split_json_func: typing.Callable,
                        force_redo: bool):
    """
    A parameterized split_json_... function unit test executor.
    """
    s3_client = boto_ses.client("s3")
    # Create single source csv file if not exists
    if (force_redo) or (not is_s3_object_exists(s3_client, source_bucket, source_key)):
        create_s3_json_file(
            boto_ses=boto_ses,
            n_k_rows=n_k_rows,
            bucket=source_bucket,
            key=source_key,
        )

    # If first target not exists, execute split csv
    first_target_key = target_key.format(i=1)
    if (force_redo) or (not is_s3_object_exists(s3_client, target_bucket, first_target_key)):
        split_json_func(
            s3_client,
            source_bucket,
            source_key,
            target_bucket,
            target_key,
            target_size_or_rows,
        )

    # Verify small target json files
    common_target_key_prefix = target_key.replace("{i}.json", "")
    response = s3_client.list_objects(Bucket=target_bucket, Prefix=common_target_key_prefix)
    n_rows_total = 0
    previous_last_id = None
    for nth_file, obj_meta in enumerate(response["Contents"]):
        nth_file += 1
        key = obj_meta["Key"]
        df = wr.s3.read_json(
            path=f"s3://{target_bucket}/{key}",
            orient="records",
            lines=True,
        )
        n_rows = df.shape[0]
        first_id = df["id"].head(1).tolist()[0]
        last_id = df["id"].tail(1).tolist()[0]
        n_rows_total += n_rows
        if nth_file != 1:
            assert previous_last_id + 1 == first_id
        previous_last_id = last_id
    assert n_rows_total == n_k_rows * 1000


def run_test_merge_parquet(boto_ses,
                           n_files: int,
                           n_rows_per_file: int,
                           source_bucket: str,
                           source_key: str,
                           target_bucket: str,
                           target_key: str,
                           target_size: int,
                           force_redo: bool):
    s3_client = boto_ses.client("s3")
    # Create many parquet test dummy data
    create_many_parquet_file(
        boto_ses,
        n_files=n_files,
        n_rows_per_file=n_rows_per_file,
        bucket=source_bucket,
        key=source_key,
        overwrite=True,
    )

    # Merge files
    common_source_key_prefix = source_key.replace("{i}.parquet", "")
    merge_parquet_by_prefix(
        boto3_session=boto_ses,
        source_bucket=source_bucket,
        source_key_prefix=common_source_key_prefix,
        target_bucket=target_bucket,
        target_key=target_key,
        target_size=target_size,
    )

    # Verify merged parquet files
    common_target_key_prefix = target_key.replace("{i}.parquet", "")
    response = s3_client.list_objects(Bucket=target_bucket, Prefix=common_target_key_prefix)
    n_rows_total = 0
    previous_last_id = None
    for nth_file, obj_meta in enumerate(response["Contents"]):
        nth_file += 1
        key = obj_meta["Key"]
        df = wr.s3.read_parquet(path=f"s3://{target_bucket}/{key}", boto3_session=boto_ses)
        n_rows = df.shape[0]
        first_id = df["id"].head(1).tolist()[0]
        last_id = df["id"].tail(1).tolist()[0]
        n_rows_total += n_rows
        if nth_file != 1:
            assert previous_last_id + 1 == first_id
        previous_last_id = last_id
    assert n_rows_total == n_files * n_rows_per_file


def run_test_merge_json(boto_ses,
                        n_files: int,
                        n_rows_per_file: int,
                        source_bucket: str,
                        source_key: str,
                        target_bucket: str,
                        target_key: str,
                        target_size: int,
                        force_redo: bool):
    s3_client = boto_ses.client("s3")
    # Create many parquet test dummy data
    create_many_json_file(
        boto_ses,
        n_files=n_files,
        n_rows_per_file=n_rows_per_file,
        bucket=source_bucket,
        key=source_key,
        overwrite=True,
    )

    # Merge files
    common_source_key_prefix = source_key.replace("{i}.json", "")
    merge_json_by_prefix(
        s3_client=s3_client,
        source_bucket=source_bucket,
        source_key_prefix=common_source_key_prefix,
        target_bucket=target_bucket,
        target_key=target_key,
        target_size=target_size,
    )

    # Verify merged parquet files
    common_target_key_prefix = target_key.replace("{i}.json", "")
    response = s3_client.list_objects(Bucket=target_bucket, Prefix=common_target_key_prefix)
    n_rows_total = 0
    previous_last_id = None
    for nth_file, obj_meta in enumerate(response["Contents"]):
        nth_file += 1
        key = obj_meta["Key"]
        df = wr.s3.read_json(
            path=f"s3://{target_bucket}/{key}",
            orient="records",
            lines=True,
            boto3_session=boto_ses,
        )
        n_rows = df.shape[0]
        first_id = df["id"].head(1).tolist()[0]
        last_id = df["id"].tail(1).tolist()[0]
        n_rows_total += n_rows
        if nth_file != 1:
            assert previous_last_id + 1 == first_id
        previous_last_id = last_id
    assert n_rows_total == n_files * n_rows_per_file
