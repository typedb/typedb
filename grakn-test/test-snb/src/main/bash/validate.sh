#!/bin/bash

source snb-env.sh

# set script directory as working directory
SCRIPTPATH=`cd "$(dirname "$0")" && pwd -P`

BRANCH_NAME=$1

LDBC_CONNECTOR=${WORKSPACE}/grakn-test/test-snb/target/test-snb-${BRANCH_NAME}-jar-with-dependencies.jar

# execute validation
java -cp ${LDBC_DRIVER}:${LDBC_CONNECTOR} com.ldbc.driver.Client -db ai.grakn.GraknDb -P ${LDBC_VALIDATION_CONFIG} -vdb ${CSV_DATA}/validation_params.csv -p ldbc.snb.interactive.parameters_dir ${CSV_DATA} -p ai.grakn.uri ${ENGINE} -p ai.grakn.keyspace ${KEYSPACE}

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
