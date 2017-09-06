#!/usr/bin/env bash

source env.sh

mvn clean package assembly:single -Dmaven.repo.local=${WORKSPACE}/maven -DskipTests -Dcheckstyle.skip=true -Dfindbugs.skip=true -Dpmd.skip=true
