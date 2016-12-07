#!/bin/bash
if [ -z "${GRAKN_HOME}" ]; then
    [[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
    GRAKN_BIN=$(cd "$(dirname "${path}")" && pwd -P)
    GRAKN_HOME=$(cd "${GRAKN_BIN}"/.. && pwd -P)
fi

CONCATCLASSPATH=$CLASSPATH":"${GRAKN_HOME}/lib/*"

if [ $1 == "csv" ]
then
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/bin" ai.grakn.migration.csv.Main ${1+"$@"}
elif [ $1 == "json" ]
then
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/bin" ai.grakn.migration.json.Main ${1+"$@"}
elif [ $1 == "owl" ]
then
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/bin" ai.grakn.migration.owl.Main ${1+"$@"}
elif [ $1 == "export" ]
then
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/bin" ai.grakn.migration.export.Main ${1+"$@"}
elif [ $1 == "sql" ]
then
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/bin" ai.grakn.migration.sql.Main ${1+"$@"}
else
  echo "usage: ./migration.sh {owl, csv, json, export, sql} <params>"
fi
