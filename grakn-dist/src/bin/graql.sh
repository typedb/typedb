#!/bin/bash

if [ -z "${GRAKN_HOME}" ]; then
    [[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
    GRAKN_BIN=$(cd "$(dirname "${path}")" && pwd -P)
    GRAKN_HOME=$(cd "${GRAKN_BIN}"/.. && pwd -P)
fi

java -cp "${GRAKN_HOME}/lib/*" -Dlogback.configurationFile="${GRAKN_HOME}/conf/main/logback.xml" -Dgrakn.log.file.main="${GRAKN_HOME}/logs/grakn.log" -Dgrakn.dir="${GRAKN_HOME}/bin" ai.grakn.graql.GraqlShell ${1+"$@"}