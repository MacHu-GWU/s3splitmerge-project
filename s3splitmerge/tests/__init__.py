# -*- coding: utf-8 -*-

from .aws import (
    bucket, boto_ses, s3_client,
)

tests_s3_key_prefix = "s3splitmerge/tests"
prefix = tests_s3_key_prefix




# class TestDataCreator:
#     def create_csv_1GB(self):
#         n_lines_per_df = 1000
#         n_df = 1000
#         columns = ["id", "value"]
#         value = "a" * 128
#         f = Path(__file__).change(new_basename="csv-1GB.csv")
#         if not f.exists():
#             for nth_df in range(n_df):
#                 df = pd.DataFrame(columns)
#                 data = list()
#                 for nth_line in range(nth_df * n_lines_per_df, (nth_df + 1) * n_lines_per_df):
#                     data.append((nth_line, value))
#                 df = pd.DataFrame(data, columns=columns)
#                 if f.exists():
#                     header = False
#                 else:
#                     header = True
#                 df.to_csv(
#                     f.abspath,
#                     index=False,
#                     header=header,
#                     mode="a",
#                 )
#         with open("")