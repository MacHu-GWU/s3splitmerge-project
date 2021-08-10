# -*- coding: utf-8 -*-

import pytest
from s3splitmerge.split import split_csv_by_size, split_csv_by_rows
from s3splitmerge.tests import bucket, prefix, boto_ses
from s3splitmerge.tests.run import run_test_split_csv


def test_split_csv_no_compress_has_header():
    run_test_split_csv(
        boto_ses=boto_ses,
        n_k_rows=1,  # 130KB
        header=True,
        source_bucket=bucket,
        source_key=f"{prefix}/split/csv/no-compress-has-header/data.csv",
        target_bucket=bucket,
        target_key=f"{prefix}/split/csv/no-compress-has-header/by-size/data-{{i}}.csv",
        target_size_or_rows=50 * 1024,  # 50KB each
        split_csv_func=split_csv_by_size,
        force_redo=True,
    )

    run_test_split_csv(
        boto_ses=boto_ses,
        n_k_rows=1,  # 130KB
        header=True,
        source_bucket=bucket,
        source_key=f"{prefix}/split/csv/no-compress-has-header/data.csv",
        target_bucket=bucket,
        target_key=f"{prefix}/split/csv/no-compress-has-header/by-rows/data-{{i}}.csv",
        target_size_or_rows=300,  # 300 lines / 40KB each
        split_csv_func=split_csv_by_rows,
        force_redo=True,
    )


def test_split_csv_no_compress_no_header():
    run_test_split_csv(
        boto_ses=boto_ses,
        n_k_rows=1,  # 130KB
        header=False,
        source_bucket=bucket,
        source_key=f"{prefix}/split/csv/no-compress-no-header/data.csv",
        target_bucket=bucket,
        target_key=f"{prefix}/split/csv/no-compress-no-header/by-size/data-{{i}}.csv",
        target_size_or_rows=50 * 1024,  # 50KB each
        split_csv_func=split_csv_by_size,
        force_redo=True,
    )

    run_test_split_csv(
        boto_ses=boto_ses,
        n_k_rows=1,  # 130KB
        header=False,
        source_bucket=bucket,
        source_key=f"{prefix}/split/csv/no-compress-no-header/data.csv",
        target_bucket=bucket,
        target_key=f"{prefix}/split/csv/no-compress-no-header/by-rows/data-{{i}}.csv",
        target_size_or_rows=300,  # 300 lines / 40KB each
        split_csv_func=split_csv_by_rows,
        force_redo=True,
    )


if __name__ == "__main__":
    import os

    basename = os.path.basename(__file__)
    pytest.main([basename, "-s", "--tb=native"])
