# -*- coding: utf-8 -*-

"""
Find out how to merge big parquet file with low memory usage.
"""

import io
from s3splitmerge.tests import boto_ses
import pyarrow
import pyarrow.parquet
import pandas as pd
import awswrangler as wr

value = "Hello World"
columns = ["id", "value"]
df1 = pd.DataFrame(
    [
        (1, value),
        (2, value),
    ],
    columns=columns
)
df2 = pd.DataFrame(
    [
        (3, value),
        (4, value),
    ],
    columns=columns
)


def create_many_files():
    df1.to_parquet("data-1.parquet", index=False)
    df2.to_parquet("data-2.parquet", index=False)


def concatenate_files():
    buffer = io.BytesIO()
    with open("append-parquet-file/data.parquet", "wb") as f_out:
        for filename in ["data-1.parquet", "data-2.parquet"]:
            with open(filename, "rb") as f_in:
                buffer.write(f_in.read())
        f_out.write(buffer.getvalue())


def concatenate_in_memory():
    buffer = io.BytesIO()
    pqwriter = pyarrow.parquet.ParquetWriter(
        buffer,
        pyarrow.Table.from_pandas(df1).schema,
    )

    for df in [df1, df2]:
        t = pyarrow.Table.from_pandas(df)
        pqwriter.write_table(t)
    pqwriter.close()

    with open("append-parquet-file/data.parquet", "wb") as f:
        f.write(buffer.getvalue())


def concat_s3_parquet_file():
    wr.s3.to_parquet(
        df1,
        path="s3://aws-data-lab-sanhe-aws-etl-solutions/s3splitmerge/poc/merge-parquet/data-1.parquet",
        boto3_session=boto_ses,
    )
    wr.s3.to_parquet(
        df2,
        path="s3://aws-data-lab-sanhe-aws-etl-solutions/s3splitmerge/poc/merge-parquet/data-2.parquet",
        boto3_session=boto_ses,
    )

    df1_ = wr.s3.read_parquet("s3://aws-data-lab-sanhe-aws-etl-solutions/s3splitmerge/poc/merge-parquet/data-1.parquet")
    df2_ = wr.s3.read_parquet("s3://aws-data-lab-sanhe-aws-etl-solutions/s3splitmerge/poc/merge-parquet/data-2.parquet")

    buffer = io.BytesIO()
    parquet_writer = pyarrow.parquet.ParquetWriter(
        buffer,
        pyarrow.Table.from_pandas(df1).schema,
    )

    for df in [df1, df2]:
        t = pyarrow.Table.from_pandas(df)
        parquet_writer.write_table(t)
    parquet_writer.close()

    # with open("data.parquet", "wb") as f:
    #     f.write(buffer.getvalue())
    # with open("data.parquet", "rb") as f:
    #     s3_client.put_object(Bucket="aws-data-lab-sanhe-aws-etl-solutions", Key="s3splitmerge/poc/merge-parquet/data.parquet", Body=f.read())
    # df = wr.s3.read_parquet("s3://aws-data-lab-sanhe-aws-etl-solutions/s3splitmerge/poc/merge-parquet/data.parquet")

    # with open("data.parquet", "wb") as f:
    #     f.write(buffer.getvalue())
    df = wr.p.read_parquet("data.parquet")

    print(df)


if __name__ == "__main__":
    # create_many_files()
    # concatenate_files()
    # concatenate_in_memory()
    # df = pd.read_parquet("data-11.parquet")
    # print(df)

    # concat_s3_parquet_file()

    print(df1.append(df2))
    print(df1)
    pass
