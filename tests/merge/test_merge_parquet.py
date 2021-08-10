# -*- coding: utf-8 -*-

import pytest
from s3splitmerge.tests import bucket, boto_ses
from s3splitmerge.tests.run import run_test_merge_parquet


def test_merge_parquet_by_prefix():
    run_test_merge_parquet(
        boto_ses=boto_ses,
        n_files=10,
        n_rows_per_file=1000,
        source_bucket=bucket,
        source_key="s3splitmerge/tests/merge/parquet/input/data-{i}.parquet",
        target_bucket=bucket,
        target_key="s3splitmerge/tests/merge/parquet/output/by-prefix/data-{i}.parquet",
        target_size=100 * 1024,
        force_redo=True,
    )


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
