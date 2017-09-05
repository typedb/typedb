#!/usr/bin/env bash

# force script to exit on failed command
set -e

# validate the number of arguments
if [ "$#" -lt "2" ]; then
	echo "Wrong number of arguments." >&2
	exit 1
fi

WORKSPACE=$1
BRANCH_NAME=$2

npm config set registry http://registry.npmjs.org/

if [ -d maven ] ;  then rm -rf maven ; fi

mvn versions:set -DnewVersion=$BRANCH_NAME -DgenerateBackupPoms=false

mvn clean install -Dmaven.repo.local=$WORKSPACE/maven -DskipTests -U -Djetty.log.level=WARNING -Djetty.log.appender=STDOUT
