#!/usr/bin/env bash

source env.sh

pushd grakn-test/test-snb
mvn clean package assembly:single --batch-mode -Dmaven.repo.local=${WORKSPACE}/maven -DskipTests -Dcheckstyle.skip=true -Dfindbugs.skip=true -Dpmd.skip=true
popd
