# -*- coding: utf-8 -*-

from pathlib_mate import Path
import io
import pandas as pd
from split_merge_0_lib import dir_tmp, clear_tmp

clear_tmp()


def append_write():
    df1 = pd.DataFrame({"id": [1, 2, 3]})
    df2 = pd.DataFrame({"id": [4, 5, 6]})

    # to file
    df1.to_csv(Path(dir_tmp, "append_write_to_file_test.csv"), index=False, mode="a")
    df2.to_csv(Path(dir_tmp, "append_write_to_file_test.csv"), index=False, header=False, mode="a")

    # to buffer
    buffer = io.StringIO()
    df1.to_csv(buffer, index=False, mode="a")
    df2.to_csv(buffer, index=False, header=False, mode="a")
    Path(dir_tmp, "append_write_to_buffer_text.csv").write_text(buffer.getvalue())


# append_write()
