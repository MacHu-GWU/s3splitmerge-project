# -*- coding: utf-8 -*-

import pytest
from s3splitmerge.split import split_json_by_size, split_json_by_rows
from s3splitmerge.tests import bucket, prefix, boto_ses
from s3splitmerge.tests.run import run_test_split_json


def test_split_json_no_compress():
    run_test_split_json(
        boto_ses=boto_ses,
        n_k_rows=1,  # 130KB
        source_bucket=bucket,
        source_key=f"{prefix}/split/json/no-compress/data.json",
        target_bucket=bucket,
        target_key=f"{prefix}/split/json/no-compress/by-size/data-{{i}}.json",
        target_size_or_rows=50 * 1024,  # 50KB each
        split_json_func=split_json_by_size,
        force_redo=True,
    )

    run_test_split_json(
        boto_ses=boto_ses,
        n_k_rows=1,  # 130KB
        source_bucket=bucket,
        source_key=f"{prefix}/split/json/no-compress/data.json",
        target_bucket=bucket,
        target_key=f"{prefix}/split/json/no-compress/by-rows/data-{{i}}.json",
        target_size_or_rows=350,  # 350 lines / 40KB each
        split_json_func=split_json_by_rows,
        force_redo=True,
    )


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
