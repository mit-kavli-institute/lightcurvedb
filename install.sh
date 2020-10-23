#! /usr/bin/bash

current_branch=$(git branch | sed -n '/\* /s///p')
lightcurvedb_path="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"  # Will fail on symlinked directories!

echo "Installing lightcurvedb on branch ${current_branch}"

git remote update
cd $lightcurvedb_path
pip install --user .
