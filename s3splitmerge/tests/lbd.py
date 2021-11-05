# -*- coding: utf-8 -*-

import attr
from ..lbd.event import SplitEvent, MergeEvent
from .aws import bucket, prefix


class PerformanceTestEventDataMaker:
    def split_csv_by_size(self) -> dict:
        """
        Test case:

        - 7,700,000 lines, 1GB file
        - each line is: ``id,value``
        - id = 1 ~ 7,700,000
        - value = "a" * 128
        - split into 20 * 50MB file

        Performance:

        - time: 116 secs
        - max memory used: 134 MB
        """
        return SplitEvent(
            source_bucket=bucket,
            source_key=f"{prefix}/big-file/csv-1GB.csv",
            target_bucket=bucket,
            target_key=f"{prefix}/big-file/csv-1GB-{{i}}.csv",
            header=True,
            target_size=50 * 1024 * 1024,
        ).to_dict()

    def split_csv_by_rows(self) -> dict:
        """
        Test case:

        - 7,700,000 lines, 1GB file
        - each line is: ``id,value``
        - id = 1 ~ 7,700,000
        - value = "a" * 128
        - split into 20 * 50MB file

        Result:

        - time: 71 secs
        - max memory used: 134 MB
        """
        return SplitEvent(
            source_bucket="aws-data-lab-sanhe-aws-etl-solutions",
            source_key="s3splitmerge/tests/big-file/csv-1GB.csv",
            target_bucket="aws-data-lab-sanhe-aws-etl-solutions",
            target_key="s3splitmerge/tests/big-file/csv-1GB-{i}.csv",
            header=True,
            target_rows=375 * 1000,
        ).to_dict()


event_maker = PerformanceTestEventDataMaker()
