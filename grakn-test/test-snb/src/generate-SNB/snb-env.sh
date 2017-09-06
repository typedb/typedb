#!/bin/bash

source env.sh

VALIDATION_DATA=${WORKSPACE}/generate-SNB/readwrite_neo4j--validation_set.tar.gz
KEYSPACE=snb
ENGINE=localhost:4567
ACTIVE_TASKS=1000
