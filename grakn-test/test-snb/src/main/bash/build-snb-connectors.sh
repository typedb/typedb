#!/usr/bin/env bash

source env.sh

mvn clean package --also-make --projects grakn-test/test-snb --batch-mode -Dmaven.repo.local=${WORKSPACE}/maven -DskipTests -Dcheckstyle.skip=true -Dfindbugs.skip=true -Dpmd.skip=true
