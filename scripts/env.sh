#!/usr/bin/env bash

set -e

cd ..

# validate the number of arguments
if [ "$#" -ne "1" ]; then
	echo "Needs one argument that is the module name" >&2
	exit 1
fi

MODULE_NAME=$1

export WORKSPACE=`pwd`

PATH="${WORKSPACE}/grakn-test/test-integration/src/test/bash:${WORKSPACE}/grakn-test/${MODULE_NAME}:${WORKSPACE}/grakn-test/${MODULE_NAME}/src/main/bash:${PATH}"
