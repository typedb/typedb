#!/usr/bin/env bash

source env.sh

# this a bash trick to specify the default value
args=${@:-arch validate}

wget https://github.com/ldbc/ldbc_snb_interactive_validation/raw/master/neo4j/readwrite_neo4j--validation_set.tar.gz
load-SNB.sh ${args}
measure-size.sh