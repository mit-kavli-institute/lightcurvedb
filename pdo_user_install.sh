#! /usr/bin/bash

current_branch=$(git branch | sed -n '/\* /s///p')
lightcurvedb_path="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"  # Will fail on symlinked directories!
script_path="${lightcurvedb_path}/install.sh"
echo "Installing lightcurvedb: ${current_branch} via pdodev with ${script_path}"
ssh -t pdodev "./${script_path}"
