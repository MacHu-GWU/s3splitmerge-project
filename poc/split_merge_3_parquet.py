# -*- coding: utf-8 -*-

from pathlib_mate import Path
import io
from fastparquet import write, ParquetFile
import pandas as pd
import awswrangler as wr
from s3splitmerge.tests.aws import s3_client, bucket, prefix
from s3splitmerge.tests.data import rand_str
from split_merge_0_lib import dir_tmp, clear_tmp

clear_tmp()


def append_write():
    df1 = pd.DataFrame({"id": [1, 2, 3]})
    df2 = pd.DataFrame({"id": [4, 5, 6]})

    # to file
    write(Path(dir_tmp, "append_write_to_parquet_file_test.parquet").abspath, df1, file_scheme="hive", append=False)
    write(Path(dir_tmp, "append_write_to_parquet_file_test.parquet").abspath, df2, file_scheme="hive", append=True)

    pq = ParquetFile(Path(dir_tmp, "append_write_to_parquet_file_test.parquet").abspath)
    df = pq.to_pandas()
    print(df)


# append_write()


def iter_local_parquet_row_group():
    """
    测试 当文件在本地磁盘时, 如何能够一次只把一个 row group 读取到内存.
    """
    df = pd.DataFrame({"id": range(1, 1000000)})
    p_before = Path(dir_tmp, "split_parquet_before.parquet")
    df.to_parquet(p_before, row_group_size=1000)

    pf = ParquetFile(p_before.abspath)
    total_byte_size = 0
    total_compressed_size = 0
    for ind, rg in enumerate(pf.row_groups):
        total_byte_size += rg.total_byte_size
        total_compressed_size += rg.total_compressed_size
        print(f"{ind+1}th row group")
        print(f"rg.num_rows = {rg.num_rows}")
        print(f"rg.total_byte_size = {rg.total_byte_size}")
        print(f"rg.total_compressed_size = {rg.total_compressed_size}")

    print(f"total_byte_size = {total_byte_size}")
    print(f"total_compressed_size = {total_compressed_size}")


# iter_local_parquet_row_group()


def iter_s3_parquet_row_group():
    """
    测试 当文件在本地磁盘时, 如何能够一次只把一个 row group 读取到内存.
    """
    import s3fs
    n = 1000000 * 1
    df = pd.DataFrame({"id": range(1, 1+n)})
    df["text"] = [rand_str(32) for _ in range(n)]
    key = f"{prefix}/poc/split_parquet_before.parquet"
    df.to_parquet("split_parquet_before.parquet", row_group_size=100000)
    s3_client.upload_file(
        "split_parquet_before.parquet",bucket, key
    )


    # buffer = io.BytesIO()
    # df.to_parquet(buffer, row_group_size=100000)
    # s3_client.put_object(
    #     Bucket=bucket,
    #     Key=key,
    #     Body=buffer.getvalue(),
    # )

    # s3 = s3fs.S3FileSystem()
    # myopen = s3.open
    # pf = ParquetFile(f"/{bucket}/{key}", open_with=myopen)
    # for df in pf.iter_row_groups():
    #     print(df.shape)


iter_s3_parquet_row_group()