#!/bin/bash

if [ -z "${GRAKN_HOME}" ]; then
    [[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
    GRAKN_BIN=$(cd "$(dirname "${path}")" && pwd -P)
    GRAKN_HOME=$(cd "${GRAKN_BIN}"/.. && pwd -P)
fi

# Define CLASSPATH
for jar in "${GRAKN_HOME}"/services/lib/*.jar; do
   CLASSPATH="$CLASSPATH:$jar"
done

# Add path containing logback.xml
CLASSPATH="$CLASSPATH":"${GRAKN_HOME}"/conf

java -cp ${CLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/services" ai.grakn.graql.GraqlShell ${1+"$@"}
