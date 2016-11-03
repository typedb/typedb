#!/bin/bash
[[ $(readlink $0) ]] && path=$(readlink $0) || path=$0

CONCATCLASSPATH=$CLASSPATH":`dirname $path`/../lib/*"
SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`


if [ $1 == "csv" ]
then
  java -cp ${CONCATCLASSPATH} -Dmindmaps.dir=${SCRIPTPATH} io.mindmaps.migration.csv.Main ${1+"$@"}
elif [ $1 == "json" ]
then
  java -cp ${CONCATCLASSPATH} -Dmindmaps.dir=${SCRIPTPATH} io.mindmaps.migration.json.Main ${1+"$@"}
elif [ $1 == "owl" ]
then
  java -cp ${CONCATCLASSPATH} -Dmindmaps.dir=${SCRIPTPATH} io.mindmaps.migration.owl.Main ${1+"$@"}
elif [ $1 == "export" ]
then
  java -cp ${CONCATCLASSPATH} -Dmindmaps.dir=${SCRIPTPATH} io.mindmaps.migration.export.Main ${1+"$@"}
else
  echo "usage: ./migration.sh {owl, csv, json, export} <params>"
fi
