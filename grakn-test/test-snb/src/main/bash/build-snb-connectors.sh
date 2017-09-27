#!/usr/bin/env bash

source env.sh

mvn clean package \
    --projects grakn-test/test-snb \
    --also-make \
    --batch-mode \
    -Dmaven.repo.local=${WORKSPACE}/maven \
    -DskipTests \
    -Dcheckstyle.skip=true -Dfindbugs.skip=true -Dpmd.skip=true
