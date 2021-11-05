# -*- coding: utf-8 -*-

import pytest
from s3splitmerge.tests import s3_client, data as data_creator
from s3splitmerge.helpers import parse_s3_uri, get_s3_object_metadata

delta = 0.05


def assert_approx(v, expected, delta):
    assert abs(v - expected) / expected <= delta


def test_create_csv_1MB():
    s3_uri = data_creator.create_csv_1MB(overwrite=False)
    bucket, key = parse_s3_uri(s3_uri)
    meta = get_s3_object_metadata(s3_client=s3_client, bucket=bucket, key=key)
    MB = 1024 ** 2
    assert_approx(meta.size, MB, delta)


def test_create_csv_1GB():
    s3_uri = data_creator.create_csv_1GB(overwrite=False)
    bucket, key = parse_s3_uri(s3_uri)
    meta = get_s3_object_metadata(s3_client=s3_client, bucket=bucket, key=key)
    GB = 1024 ** 3
    assert_approx(meta.size, GB, delta)


def test_create_json_1MB():
    s3_uri = data_creator.create_json_1MB(overwrite=False)
    bucket, key = parse_s3_uri(s3_uri)
    meta = get_s3_object_metadata(s3_client=s3_client, bucket=bucket, key=key)
    MB = 1024 ** 2
    assert_approx(meta.size, MB, delta)


def test_create_json_1GB():
    s3_uri = data_creator.create_json_1GB(overwrite=False)
    bucket, key = parse_s3_uri(s3_uri)
    meta = get_s3_object_metadata(s3_client=s3_client, bucket=bucket, key=key)
    GB = 1024 ** 3
    assert_approx(meta.size, GB, delta)


def test_create_parquet_1MB():
    s3_uri = data_creator.create_parquet_1MB(overwrite=False)
    bucket, key = parse_s3_uri(s3_uri)
    meta = get_s3_object_metadata(s3_client=s3_client, bucket=bucket, key=key)
    MB = 1024 ** 2
    assert_approx(meta.size, MB, delta)


def test_create_parquet_1GB():
    s3_uri = data_creator.create_parquet_1GB(overwrite=True)
    bucket, key = parse_s3_uri(s3_uri)
    meta = get_s3_object_metadata(s3_client=s3_client, bucket=bucket, key=key)
    GB = 1024 ** 3
    assert_approx(meta.size, GB, delta)


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
