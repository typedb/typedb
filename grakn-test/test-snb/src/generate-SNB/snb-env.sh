#!/bin/bash

source env.sh

VALIDATION_DATA=${WORKSPACE}/generate-SNB/readwrite_neo4j--validation_set.tar.gz
CSV_DATA=${WORKSPACE}/generate-SNB/social_network
LDBC_DRIVER=${WORKSPACE}/.m2/repository/com/ldbc/driver/jeeves/0.3-SNAPSHOT/jeeves-0.3-SNAPSHOT.jar
KEYSPACE=snb
ENGINE=localhost:4567
ACTIVE_TASKS=1000
