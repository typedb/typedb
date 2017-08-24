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

# Add path containing grakn.properties and logback.xml
CLASSPATH="$CLASSPATH":"${GRAKN_HOME}"/conf
CLASSPATH="$CLASSPATH":"${GRAKN_HOME}"/services/grakn

if [ "$1" == "csv" ]
then
  java -cp ${CLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/services" ai.grakn.migration.csv.CSVMigrator ${1+"$@"}
elif [ "$1" == "json" ]
then
  java -cp ${CLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/services" ai.grakn.migration.json.JsonMigrator ${1+"$@"}
elif [ "$1" == "owl" ]
then
  java -cp ${CLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/services" ai.grakn.migration.owl.Main ${1+"$@"}
elif [ "$1" == "export" ]
then
  java -cp ${CLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/services" ai.grakn.migration.export.Main ${1+"$@"}
elif [ "$1" == "sql" ]
then
  java -cp ${CLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/services" ai.grakn.migration.sql.SQLMigrator ${1+"$@"}
elif [ "$1" == "xml" ]
then
  java -cp ${CLASSPATH} -Dgrakn.dir="${GRAKN_HOME}/services" ai.grakn.migration.xml.XmlMigrator ${1+"$@"}
else
  echo "usage: ./migration.sh {owl, csv, json, export, sql, xml} <params>"
fi
