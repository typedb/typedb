#!/bin/bash
[[ $(readlink $0) ]] && path=$(readlink $0) || path=$0

SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`

if [ $1 == "csv" ]
then
  echo "csv migration starting"
  java -cp "`dirname $path`/../lib/*" -Dmindmaps.dir=$SCRIPTPATH io.mindmaps.migration.csv.Main ${1+"$@"}
elif [ $1 == "sql" ]
then
  echo "sql migration starting"
  java -cp "`dirname $path`/../lib/*" -Dmindmaps.dir=$SCRIPTPATH io.mindmaps.migration.sql.Main ${1+"$@"}
elif [ $1 == "json" ]
then
  echo "json migration starting"
  java -cp "`dirname $path`/../lib/*" -Dmindmaps.dir=$SCRIPTPATH io.mindmaps.migration.json.Main ${1+"$@"}
elif [ $1 == "owl" ]
then
  echo "owl migration starting"
  java -cp "`dirname $path`/../lib/*" -Dmindmaps.dir=$SCRIPTPATH io.mindmaps.migration.owl.Main ${1+"$@"}
elif [ $1 == "export" ]
then
  echo "export starting"
  java -cp "`dirname $path`/../lib/*" -Dmindmaps.dir=$SCRIPTPATH io.mindmaps.migration.export.Main ${1+"$@"}
else
  echo "usage: ./migration.sh {owl, sql, csv, json, export} <params>"
fi

