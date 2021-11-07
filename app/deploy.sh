#!/bin/bash
# -*- coding: utf-8 -*-
#
# Build lambda deployment package in container locally

dir_here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
dir_project_root="$(dirname "${dir_here}")"

source "${dir_project_root}/bin/lbd/lambda-env.sh"

cd "${dir_project_root}/app"
rm -r "${dir_project_root}/app/vendor/s3splitmerge"
cp -r "${dir_project_root}/s3splitmerge" "${dir_project_root}/app/vendor/s3splitmerge"

${bin_chalice} deploy
