#!/bin/bash
#
# NOTE: This script should be executed INSIDE of the container

dir_here="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
dir_bin="$(dirname "${dir_here}")"
dir_project_root=$(dirname "${dir_bin}")

source ${dir_bin}/lbd/lambda-env.sh

resolve_important_path ${dir_project_root}
resolve_linux_venv ${venv_name} ${py_version} ${py_version_major_and_minor}

rm_if_exists ${dir_venv}
rm_if_exists ${path_lambda_deploy_pkg_file}
rm_if_exists ${path_run_lambda_site_packages}

cd ${dir_project_root}

print_colored_line $color_cyan "create virtual env at ${dir_venv} in container ..."
virtualenv ${dir_venv}

print_colored_line $color_cyan "pip install dependencies ..."
${bin_pip} install ${dir_project_root}
${bin_pip} install -r ${dir_project_root}/requirements-awswrangler.txt
${bin_pip} install awswrangler==2.10.0 --no-deps

build_lbd_deployment_package

print_colored_line $color_cyan "done"

rm_if_exists ${dir_venv}
