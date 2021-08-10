# -*- coding: utf-8 -*-

import pytest
from pytest import raises, approx
from s3splitmerge import exc
from s3splitmerge.helpers import (
    get_s3_object_metadata,
)
from s3splitmerge.tests import bucket, tests_s3_key_prefix, s3_client


def test_get_s3_object_metadata():
    with raises(exc.S3ObjectNotFound):
        get_s3_object_metadata(s3_client, bucket, f"{tests_s3_key_prefix}/helpers/not-exists-object.json")

    s3_client.put_object(Bucket=bucket, Key=f"{tests_s3_key_prefix}/helpers/existing-object.json", Body='{"id": 1}')
    metadata = get_s3_object_metadata(s3_client, bucket, f"{tests_s3_key_prefix}/helpers/existing-object.json")
    assert metadata.size == 9


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
