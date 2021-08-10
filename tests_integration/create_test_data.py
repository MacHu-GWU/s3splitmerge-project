# -*- coding: utf-8 -*-

from s3splitmerge.tests import data

if __name__ == "__main__":
    data.create_csv_1MB(overwrite=True)
    data.create_csv_1GB(overwrite=False)
    data.create_json_1MB(overwrite=True)
    data.create_json_1GB(overwrite=False)
    data.create_parquet_1MB(overwrite=True)
    data.create_parquet_1GB(overwrite=False)
    pass
