#!/usr/bin/env bash

source env.sh

# validate the number of arguments
if [ "$#" -lt "1" ]; then
	echo "Wrong number of arguments." >&2
	exit 1
fi

BRANCH_NAME=$1

npm config set registry http://registry.npmjs.org/

if [ -d maven ] ;  then rm -rf maven ; fi

echo "Setting version using branch name"
mvn versions:set -DnewVersion=${BRANCH_NAME} -DgenerateBackupPoms=false

echo "Installing grakn"
mvn clean install -T 14 --batch-mode -Dmaven.repo.local=${WORKSPACE}/maven -DskipTests -U -Djetty.log.level=WARNING -Djetty.log.appender=STDOUT
