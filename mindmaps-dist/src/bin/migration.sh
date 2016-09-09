#!/bin/bash
[[ $(readlink $0) ]] && path=$(readlink $0) || path=$0

if [ $1 == "csv" ]
then
  echo "csv migration starting"
  java -cp "`dirname $path`/../lib/*" io.mindmaps.migration.csv.Main ${1+"$@"}
elif [ $1 == "sql" ]
then
  echo "sql migration starting"
  java -cp "`dirname $path`/../lib/*" io.mindmaps.migration.sql.Main ${1+"$@"}
elif [ $1 == "owl" ]
then
  echo "owl migration starting"
  java -cp "`dirname $path`/../lib/*" io.mindmaps.migration.owl.Main ${1+"$@"}
else
  echo "usage: ./migration.sh {owl, sql, csv} <params>"
fi

