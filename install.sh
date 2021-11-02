#! /usr/bin/bash

echo " __         ______     _____     ______    "
echo "/\ \       /\  ___\   /\  __-.  /\  == \   "
echo "\ \ \____  \ \ \____  \ \ \/\ \ \ \  __<   "
echo " \ \_____\  \ \_____\  \ \____-  \ \_____\ "
echo "  \/_____/   \/_____/   \/____/   \/_____/ "


PIP_PATH=${1:-"pip"}

current_branch=$(git branch | sed -n '/\* /s///p')
lightcurvedb_path="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"  # Will fail on symlinked directories!

cd $lightcurvedb_path
$PIP_PATH install --user .
