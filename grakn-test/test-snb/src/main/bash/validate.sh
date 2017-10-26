#!/bin/bash

source snb-env.sh

BRANCH_NAME=$1

build-snb-connectors.sh

LDBC_CONNECTOR=${WORKSPACE}/grakn-test/test-snb/target/test-snb-${BRANCH_NAME}-jar-with-dependencies.jar

# TODO: This is weird and possibly unnecessary now
LDBC_DRIVER=${HOME}/.m2/repository/com/ldbc/driver/jeeves/0.3-SNAPSHOT/jeeves-0.3-SNAPSHOT.jar

LDBC_VALIDATION_CONFIG=${WORKSPACE}/grakn-test/test-snb/src/main/bash/readwrite_grakn--ldbc_driver_config--db_validation.properties

# execute validation
java \
    -classpath ${LDBC_DRIVER}:${LDBC_CONNECTOR} com.ldbc.driver.Client \
    -db ai.grakn.GraknDb \
    -P ${LDBC_VALIDATION_CONFIG} \
    -vdb ${CSV_DATA}/validation_params.csv \
    -p ldbc.snb.interactive.parameters_dir ${CSV_DATA} \
    -p ai.grakn.uri ${ENGINE} \
    -p ai.grakn.keyspace ${KEYSPACE}

# check for errors from LDBC
FAILURES=$(cat ${CSV_DATA}/validation_params-failed-actual.json)
if [ "${FAILURES}" == "[ ]" ]; then
        echo "Validation completed without failures."
else
        echo "There were failures during validation."
        echo ${FAILURES}
        exit 1
fi
