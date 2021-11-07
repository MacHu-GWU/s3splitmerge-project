# -*- coding: utf-8 -*-

from pathlib_mate import Path

dir_tmp = Path(Path(__file__).parent, "tmp")

def clear_tmp():
    for p in dir_tmp.select_file():
        p.remove()
