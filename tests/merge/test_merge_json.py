# -*- coding: utf-8 -*-

import pytest
from s3splitmerge.tests import bucket, boto_ses
from s3splitmerge.tests.run import run_test_merge_json


def test_merge_json_by_prefix():
    run_test_merge_json(
        boto_ses=boto_ses,
        n_files=10,
        n_rows_per_file=1000,
        source_bucket=bucket,
        source_key="s3splitmerge/tests/merge/json/input/data-{i}.json",
        target_bucket=bucket,
        target_key="s3splitmerge/tests/merge/json/output/by-prefix/data-{i}.json",
        target_size=500 * 1024,
        force_redo=False,
    )


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
