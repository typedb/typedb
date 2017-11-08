#!/bin/bash

source snb-env.sh

build-snb-connectors.sh

LDBC_CONNECTOR=${WORKSPACE}/grakn-test/test-snb/target/test-snb-*-jar-with-dependencies.jar

LDBC_VALIDATION_CONFIG=${WORKSPACE}/grakn-test/test-snb/src/main/bash/readwrite_grakn--ldbc_driver_config--db_validation.properties

# execute validation
java \
    -classpath ${LDBC_CONNECTOR} com.ldbc.driver.Client \
    -db ai.grakn.GraknDb \
    -P ${LDBC_VALIDATION_CONFIG} \
    -vdb ${CSV_DATA}/validation_params.csv \
    -p ldbc.snb.interactive.parameters_dir ${CSV_DATA} \
    -p ai.grakn.uri ${ENGINE} \
    -p ai.grakn.keyspace ${KEYSPACE}

# check for errors from Grakn
check-errors.sh fail

# check for errors from LDBC
FAILURES=$(cat ${CSV_DATA}/validation_params-failed-actual.json)
if [ "${FAILURES}" == "[ ]" ]; then
        echo "Validation completed without failures."
else
        echo "There were failures during validation."
        echo ${FAILURES}
        exit 1
fi
