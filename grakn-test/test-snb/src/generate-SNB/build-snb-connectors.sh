#!/usr/bin/env bash

# force script to exit on failed command
set -e

WORKSPACE=$1

mvn clean package assembly:single -Dmaven.repo.local=${WORKSPACE}/maven -DskipTests -Dcheckstyle.skip=true -Dfindbugs.skip=true -Dpmd.skip=true
