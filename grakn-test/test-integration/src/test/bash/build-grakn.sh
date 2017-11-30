#!/usr/bin/env bash

source env.sh

npm config set registry http://registry.npmjs.org/

if [ -d maven ] ;  then rm -rf maven ; fi

if [ "$#" -gt "0" ]; then
    echo "Setting version using branch name"
    mvn --batch-mode versions:set -DnewVersion=$1 -DgenerateBackupPoms=false
fi

echo "Installing grakn"
cat ${MVN_SETTINGS}
mvn clean deploy -T 14 --batch-mode -DskipTests -U -Djetty.log.level=WARNING -Djetty.log.appender=STDOUT -PgraknRepo -s ${MVN_SETTINGS}
