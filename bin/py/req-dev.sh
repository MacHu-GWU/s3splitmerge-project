#!/bin/bash
# -*- coding: utf-8 -*-

dir_here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
dir_bin="$(dirname "${dir_here}")"
dir_project_root="$(dirname "${dir_bin}")"

source "${dir_bin}/py/python-env.sh"

print_colored_line $color_cyan "[DOING] pip install ${path_dev_requirement_file} ..."
${bin_pip} install -r "${path_dev_requirement_file}"
