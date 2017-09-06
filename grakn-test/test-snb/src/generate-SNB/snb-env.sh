#!/bin/bash

source env.sh

VALIDATION_DATA=${WORKSPACE}/readwrite_neo4j--validation_set.tar.gz
CSV_DATA=${WORKSPACE}/social_network
LDBC_DRIVER=${WORKSPACE}/.m2/repository/com/ldbc/driver/jeeves/0.3-SNAPSHOT/jeeves-0.3-SNAPSHOT.jar
LDBC_VALIDATION_CONFIG=${WORKSPACE}/grakn-test/test-snb/src/validate-snb/readwrite_grakn--ldbc_driver_config--db_validation.properties
KEYSPACE=snb
ENGINE=localhost:4567
ACTIVE_TASKS=1000
