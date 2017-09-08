#!/bin/bash

# Force script to exit on failed command
set -e

SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`

echo "Validating Biomed Queries . . . "

python ${SCRIPTPATH}/validation/validate.py

echo "Validation Complete "
