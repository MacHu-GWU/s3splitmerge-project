#!/bin/bash
# -*- coding: utf-8 -*-
#
# Build lambda deployment package in container locally

dir_here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
dir_bin="$(dirname "${dir_here}")"
dir_project_root=$(dirname "${dir_bin}")

source "${dir_bin}/lbd/lambda-env.sh"

cd "${dir_project_root}"
rm -r "${dir_project_root}/vendor/s3splitmerge"
cp -r "${dir_project_root}/s3splitmerge" "${dir_project_root}/vendor/s3splitmerge"

chalice deploy
