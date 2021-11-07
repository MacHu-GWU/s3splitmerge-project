# -*- coding: utf-8 -*-

import awswrangler as wr
import pandas as pd
from fastparquet import ParquetFile


# df = pd.DataFrame(columns=["id", "time"])
# n = 1000
# df["id"] = range(1, n+1)
# df["time"] = pd.date_range("2000-01-01", periods=n, freq="1H")
# df.to_parquet("fq.parquet", row_group_size=50)

# df = pd.read_parquet("fq.parquet")
# print(df)

pf = ParquetFile("fq.parquet")
# df = pf.to_pandas()
# print(df)

for df in pf.iter_row_groups():
    print(df.shape)

# print(pf[0])
# print(pf.statistics)