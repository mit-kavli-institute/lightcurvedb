#! /usr/bin/bash

if [ "$1" = "help" ] || [ "$1" = "--help" ] ; then
    echo "./pdo_user_install.sh [PIP VERSION]"
    echo "\tPIP VERSION defaults to 3"
    exit 1
fi

PIP_VERSION=${1:-"3"}
PIP_PATH=$(which "pip${PIP_VERSION}" 2> /dev/null )

if [ -z $PIP_PATH ] ; then
    echo "Could not find pip version ${PIP_VERSION}!"
    exit 1
fi

current_branch=$(git branch | sed -n '/\* /s///p')
lightcurvedb_path="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"  # Will fail on symlinked directories!
script_path="${lightcurvedb_path}/install.sh"
echo "Installing lightcurvedb: ${current_branch} via pdodev using pip${PIP_VERSION}"
ssh pdodev PATH=${PATH} PYTHONPATH=${PYTHONPATH} /bin/bash << EOF
    . /etc/bashrc
    . /sw/bin/setup-pdo.sh
    export LD_LIBRARY_PATH=/sw/openssl-versions/openssl-1.1.1d/lib:/sw/python-versions/python-3.9.2/lib/:$LD_LIBRARY_PATH
    export PATH=/sw/python-versions/python-3.9.2/bin/:$PATH
    export PATH=/sw/cmake-versions/cmake-3.15.3/bin/:$PATH
    cd ${lightcurvedb_path}
    ./install.sh ${PIP_PATH}
EOF
