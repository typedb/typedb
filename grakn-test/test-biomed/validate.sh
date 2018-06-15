#!/usr/bin/env bash

source env.sh

SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`

echo "Validating Biomed Queries . . . "

python ${SCRIPTPATH}/validation/validate.py

echo "Validation Complete "
