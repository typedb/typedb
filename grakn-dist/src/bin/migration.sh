#!/bin/bash
if [ -z "${GRAKN_HOME}" ]; then
    [[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
    GRAKN_BIN=$(cd "$(dirname "${path}")" && pwd -P)
    GRAKN_HOME=$(cd "${GRAKN_BIN}"/.. && pwd -P)
fi

# Define CLASSPATH, exclude commons-csv because of a dependency clash when using migration
for jar in "${GRAKN_HOME}"/lib/*.jar; do
    if [[ $jar != *org.apache.servicemix.bundles.commons-csv* ]] ; then
        CLASSPATH="$CLASSPATH:$jar"
    fi
done

CONCATCLASSPATH=$CLASSPATH

if [ "$1" == "csv" ]
then
  java -cp ${CONCATCLASSPATH} -Dlogback.configurationFile="${GRAKN_HOME}/conf/main/logback.xml" -Dgrakn.log.file.postprocessing="${GRAKN_HOME}/logs/grakn-postprocessing.log" -Dgrakn.log.file.main="${GRAKN_HOME}/logs/grakn.log" -Dgrakn.dir="${GRAKN_HOME}/bin" ai.grakn.migration.csv.CSVMigrator ${1+"$@"}
elif [ "$1" == "json" ]
then
  java -cp ${CONCATCLASSPATH} -Dlogback.configurationFile="${GRAKN_HOME}/conf/main/logback.xml" -Dgrakn.log.file.postprocessing="${GRAKN_HOME}/logs/grakn-postprocessing.log" -Dgrakn.log.file.main="${GRAKN_HOME}/logs/grakn.log" -Dgrakn.dir="${GRAKN_HOME}/bin" ai.grakn.migration.json.JsonMigrator ${1+"$@"}
elif [ "$1" == "owl" ]
then
  java -cp ${CONCATCLASSPATH} -Dlogback.configurationFile="${GRAKN_HOME}/conf/main/logback.xml" -Dgrakn.log.file.postprocessing="${GRAKN_HOME}/logs/grakn-postprocessing.log" -Dgrakn.log.file.main="${GRAKN_HOME}/logs/grakn.log" -Dgrakn.dir="${GRAKN_HOME}/bin" ai.grakn.migration.owl.Main ${1+"$@"}
elif [ "$1" == "export" ]
then
  java -cp ${CONCATCLASSPATH} -Dlogback.configurationFile="${GRAKN_HOME}/conf/main/logback.xml" -Dgrakn.log.file.postprocessing="${GRAKN_HOME}/logs/grakn-postprocessing.log" -Dgrakn.log.file.main="${GRAKN_HOME}/logs/grakn.log" -Dgrakn.dir="${GRAKN_HOME}/bin" ai.grakn.migration.export.Main ${1+"$@"}
elif [ "$1" == "sql" ]
then
  java -cp ${CONCATCLASSPATH} -Dlogback.configurationFile="${GRAKN_HOME}/conf/main/logback.xml" -Dgrakn.log.file.postprocessing="${GRAKN_HOME}/logs/grakn-postprocessing.log" -Dgrakn.log.file.main="${GRAKN_HOME}/logs/grakn.log" -Dgrakn.dir="${GRAKN_HOME}/bin" ai.grakn.migration.sql.SQLMigrator ${1+"$@"}
elif [ "$1" == "xml" ]
then
  java -cp ${CONCATCLASSPATH} -Dlogback.configurationFile="${GRAKN_HOME}/conf/main/logback.xml" -Dgrakn.log.file.postprocessing="${GRAKN_HOME}/logs/grakn-postprocessing.log" -Dgrakn.log.file.main="${GRAKN_HOME}/logs/grakn.log" -Dgrakn.dir="${GRAKN_HOME}/bin" ai.grakn.migration.xml.XmlMigrator ${1+"$@"}
else
  echo "usage: ./migration.sh {owl, csv, json, export, sql, xml} <params>"
fi
