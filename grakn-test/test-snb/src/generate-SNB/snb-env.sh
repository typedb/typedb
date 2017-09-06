#!/bin/bash

source env.sh

VALIDATION_DATA=${WORKSPACE}/generate-SNB/readwrite_neo4j--validation_set.tar.gz
CSV_DATA=${WORKSPACE}/generate-SNB/social_network
KEYSPACE=snb
ENGINE=localhost:4567
ACTIVE_TASKS=1000
