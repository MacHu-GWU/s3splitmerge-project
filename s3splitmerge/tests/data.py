# -*- coding: utf-8 -*-

"""
Test data generator
"""

import io
import base64
import uuid
import pandas as pd
import awswrangler as wr
from pathlib_mate import Path
from .aws import bucket, boto_ses
from ..helpers import is_s3_object_exists, check_enumeration_s3_key_string
from ..options import ZFILL


def b64encode_s3_uri(s3_uri) -> str:
    return base64.b64encode(s3_uri.encode("utf8")).decode("utf8")


def b64decode_s3_uri(encoded_s3_uri) -> str:
    return base64.b64decode(encoded_s3_uri.encode("utf8")).decode("utf8")


def create_s3_csv_file(boto_ses,
                       n_k_rows: int,
                       header: bool,
                       bucket: str,
                       key: str,
                       overwrite: bool = True):
    """
    Create a csv file on S3 with dummy data.

    :param boto_ses:
    :param n_k_rows: ``n_k_rows`` * 1000 rows will be created
    :param header: if True, the csv file has header line
    :param bucket:
    :param key:
    """
    s3_client = boto_ses.client("s3")
    n_rows_per_df = 1000
    columns = ["id", "value"]
    value = "a" * 128
    s3_uri = f"s3://{bucket}/{key}"

    if (overwrite is False) and is_s3_object_exists(s3_client, bucket, key):
        return

    if n_k_rows <= 1000:  # use in memory creation
        df = pd.DataFrame(columns=columns)
        df["id"] = range(1, 1 + n_k_rows * n_rows_per_df)
        df["value"] = value
        wr.s3.to_csv(
            df=df,
            path=s3_uri,
            index=False,
            header=header,
            boto3_session=boto_ses,
        )
    else:  # dump to local temp file and multi-part upload to s3
        tmp_file = Path("/tmp", b64encode_s3_uri(s3_uri) + ".csv")
        if tmp_file.exists():
            tmp_file.remove()
        is_first_write = True
        for nth_df in range(1, 1 + n_k_rows):
            id_lower = (nth_df - 1) * n_rows_per_df + 1
            id_upper = id_lower + n_rows_per_df
            df = pd.DataFrame(columns=columns)
            df["id"] = range(id_lower, id_upper)
            df["value"] = value
            if is_first_write:
                header_arg = True and header
                is_first_write = False
            else:
                header_arg = False
            df.to_csv(
                tmp_file.abspath,
                index=False,
                header=header_arg,
                mode="a",
            )
        s3_client.upload_file(tmp_file.abspath, bucket, key)


def create_csv_1MB(overwrite):
    create_s3_csv_file(
        boto_ses=boto_ses,
        n_k_rows=7,
        header=True,
        bucket=bucket,
        key="s3splitmerge/tests/big-file/csv-1MB.csv",
        overwrite=overwrite,
    )


def create_csv_1GB(overwrite):
    create_s3_csv_file(
        boto_ses=boto_ses,
        n_k_rows=7500,
        header=True,
        bucket=bucket,
        key="s3splitmerge/tests/big-file/csv-1GB.csv",
        overwrite=overwrite,
    )


def create_s3_json_file(boto_ses,
                        n_k_rows: int,
                        bucket: str,
                        key: str,
                        overwrite: bool = True):
    """
    Create a json file on S3 with dummy data.

    :param boto_ses:
    :param n_k_rows: ``n_k_rows`` * 1000 rows will be created
    :param bucket:
    :param key:
    """
    s3_client = boto_ses.client("s3")
    n_rows_per_df = 1000
    columns = ["id", "value"]
    value = "a" * 128
    s3_uri = f"s3://{bucket}/{key}"

    if (overwrite is False) and is_s3_object_exists(s3_client, bucket, key):
        return

    if n_k_rows <= 1000:  # use in memory creation
        df = pd.DataFrame(columns=columns)
        df["id"] = range(1, 1 + n_k_rows * n_rows_per_df)
        df["value"] = value
        wr.s3.to_json(
            df=df,
            path=s3_uri,
            orient="records",
            lines=True,
            boto3_session=boto_ses,
        )
    else:  # dump to local temp file and multi-part upload to s3
        tmp_file = Path("/tmp", b64encode_s3_uri(s3_uri) + ".json")
        if tmp_file.exists():
            tmp_file.remove()
        with open(tmp_file.abspath, "a") as file:
            for nth_df in range(1, 1 + n_k_rows):
                id_lower = (nth_df - 1) * n_rows_per_df + 1
                id_upper = id_lower + n_rows_per_df
                print(id_lower, id_upper)
                df = pd.DataFrame(columns=columns)
                df["id"] = range(id_lower, id_upper)
                df["value"] = value
                buffer = io.StringIO()
                df.to_json(
                    buffer,
                    orient="records",
                    lines=True,
                )
                file.write(buffer.getvalue())
        s3_client.upload_file(tmp_file.abspath, bucket, key)


def create_json_1MB(overwrite):
    create_s3_json_file(
        boto_ses=boto_ses,
        n_k_rows=7,
        bucket=bucket,
        key="s3splitmerge/tests/big-file/json-1MB.json",
        overwrite=overwrite,
    )


def create_json_1GB(overwrite):
    create_s3_json_file(
        boto_ses=boto_ses,
        n_k_rows=6500,
        bucket=bucket,
        key="s3splitmerge/tests/big-file/json-1GB.json",
        overwrite=overwrite,
    )


def create_many_parquet_file(boto_ses,
                             n_files: int,
                             n_rows_per_file: int,
                             bucket: str,
                             key: str,
                             overwrite=True):
    s3_client = boto_ses.client("s3")
    columns = ["id", "value"]
    check_enumeration_s3_key_string(key)

    for nth_file in range(1, n_files + 1):
        file_key = key.format(i=str(nth_file).zfill(ZFILL))
        if (overwrite is False) and is_s3_object_exists(s3_client, bucket, file_key):
            continue
        lower_id = (nth_file - 1) * n_rows_per_file + 1
        upper_id = lower_id + n_rows_per_file

        df = pd.DataFrame(columns=columns)
        df["id"] = range(lower_id, upper_id)
        df["value"] = [str(uuid.uuid4()) for _ in range(n_rows_per_file)]
        s3_uri = f"s3://{bucket}/{file_key}"
        wr.s3.to_parquet(df=df, path=s3_uri, boto3_session=boto_ses)


def create_parquet_1MB(overwrite):
    create_many_parquet_file(
        boto_ses=boto_ses,
        n_files=3,
        n_rows_per_file=7500,
        bucket=bucket,
        key="s3splitmerge/tests/many-file/parquet-1MB/{i}.parquet",
        overwrite=overwrite,
    )


def create_parquet_1GB(overwrite):
    create_many_parquet_file(
        boto_ses=boto_ses,
        n_files=25,
        n_rows_per_file=1000 * 1000,
        bucket=bucket,
        key="s3splitmerge/tests/many-file/parquet-1GB/{i}.parquet",
        overwrite=overwrite,
    )


def create_many_json_file(boto_ses,
                          n_files: int,
                          n_rows_per_file: int,
                          bucket: str,
                          key: str,
                          overwrite=True):
    s3_client = boto_ses.client("s3")
    columns = ["id", "value"]
    value = "a" * 128
    check_enumeration_s3_key_string(key)

    for nth_file in range(1, n_files + 1):
        file_key = key.format(i=str(nth_file).zfill(ZFILL))
        if (overwrite is False) and is_s3_object_exists(s3_client, bucket, file_key):
            continue
        lower_id = (nth_file - 1) * n_rows_per_file + 1
        upper_id = lower_id + n_rows_per_file

        df = pd.DataFrame(columns=columns)
        df["id"] = range(lower_id, upper_id)
        df["value"] = value
        s3_uri = f"s3://{bucket}/{file_key}"
        wr.s3.to_json(df=df, path=s3_uri, orient="records", lines=True, boto3_session=boto_ses)
