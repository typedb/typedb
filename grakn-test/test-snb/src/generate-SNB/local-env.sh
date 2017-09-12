#!/bin/bash

export VALIDATION_DATA="/opt/grakn/data/readwrite_neo4j--validation_set.tar.gz"
export SF1_DATA="/opt/grakn/data/snb-data-sf1.tar.gz"
export CSV_DATA="/tmp/social-network"
export KEYSPACE="snb"
export ENGINE="localhost:4567"
export ACTIVE_TASKS="64"
export HADOOP_HOME="/opt/grakn/hadoop-2.6.0"
export LDBC_DRIVER="/opt/grakn/ldbc_driver/target/jeeves-0.3-SNAPSHOT.jar"
export LDBC_CONNECTOR="/opt/grakn/snb-interactive-grakn-stable-jar-with-dependencies.jar"
export LDBC_VALIDATION_CONFIG=readwrite_grakn--ldbc_driver_config--db_validation.properties
