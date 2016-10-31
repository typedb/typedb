#!/bin/bash
[[ $(readlink $0) ]] && path=$(readlink $0) || path=$0

CONCATCLASSPATH=$CLASSPATH":`dirname $path`/../lib/*"
SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`


if [ $1 == "csv" ]
then
  echo "csv migration starting"
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir=${SCRIPTPATH} io.grakn.migration.csv.Main ${1+"$@"}
elif [ $1 == "sql" ]
then
  echo "sql migration starting"
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir=${SCRIPTPATH} io.grakn.migration.sql.Main ${1+"$@"}
elif [ $1 == "json" ]
then
  echo "json migration starting"
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir=${SCRIPTPATH} io.grakn.migration.json.Main ${1+"$@"}
elif [ $1 == "owl" ]
then
  echo "owl migration starting"
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir=${SCRIPTPATH} io.grakn.migration.owl.Main ${1+"$@"}
elif [ $1 == "export" ]
then
  echo "owl migration starting"
  java -cp ${CONCATCLASSPATH} -Dgrakn.dir=${SCRIPTPATH} io.grakn.migration.owl.Main ${1+"$@"}
else
  echo "usage: ./migration.sh {owl, sql, csv, json, export} <params>"
fi
