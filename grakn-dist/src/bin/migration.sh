#!/bin/bash
[[ $(readlink $0) ]] && path=$(readlink $0) || path=$0

CONCATCLASSPATH=$CLASSPATH":`dirname $path`/../lib/*"
SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`


if [ $1 == "csv" ]
then
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir=${SCRIPTPATH} ai.grakn.migration.csv.Main ${1+"$@"}
elif [ $1 == "json" ]
then
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir=${SCRIPTPATH} ai.grakn.migration.json.Main ${1+"$@"}
elif [ $1 == "owl" ]
then
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir=${SCRIPTPATH} ai.grakn.migration.owl.Main ${1+"$@"}
elif [ $1 == "export" ]
then
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir=${SCRIPTPATH} ai.grakn.migration.export.Main ${1+"$@"}
elif [ $1 == "sql" ]
then
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir=${SCRIPTPATH} ai.grakn.migration.sql.Main ${1+"$@"}
else
  echo "usage: ./migration.sh {owl, csv, json, export, sql} <params>"
fi
