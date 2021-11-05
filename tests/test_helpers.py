# -*- coding: utf-8 -*-

import pytest
from pytest import raises, approx
from s3splitmerge import exc
from s3splitmerge.helpers import (
    b64encode_str, b64decode_str,
    get_s3_object_metadata,
    is_s3_object_exists,
    count_lines_in_s3_object,
)
from s3splitmerge.tests import s3_client, bucket, prefix


def test_b64_encode_decode():
    s = "s3://bucket/key"
    assert b64encode_str(b64encode_str(s)) == s


def test_get_s3_object_metadata():
    with raises(exc.S3ObjectNotFound):
        get_s3_object_metadata(
            s3_client=s3_client,
            bucket=bucket,
            key=f"{prefix}/helpers/{get_s3_object_metadata.__name__}/not-exists-object.json",
        )

    s3_client.put_object(
        Bucket=bucket,
        Key=f"{prefix}/helpers/{get_s3_object_metadata.__name__}/existing-object.json",
        Body='{"id": 1}',
    )
    metadata = get_s3_object_metadata(
        s3_client=s3_client,
        bucket=bucket,
        key=f"{prefix}/helpers/{get_s3_object_metadata.__name__}existing-object.json",
    )
    assert metadata.size == 9


def test_is_s3_object_exists():
    assert is_s3_object_exists(
        s3_client=s3_client,
        bucket=bucket,
        key=f"{prefix}/helpers/{is_s3_object_exists.__name__}/not-exists-object.json",
    ) is False
    s3_client.put_object(
        Bucket=bucket,
        Key=f"{prefix}/helpers/{is_s3_object_exists.__name__}/existing-object.json",
        Body='{"id": 1}',
    )
    assert is_s3_object_exists(
        s3_client=s3_client,
        bucket=bucket,
        key=f"{prefix}/helpers/{is_s3_object_exists.__name__}/exists-object.json",
    ) is False


# def test_count_lines_in_s3_object():
#     count_lines_in_s3_object(
#         s3_client=s3_client,
#         bucket=bucket
#     )


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
