#!/usr/bin/env bash

source env.sh

${PACKAGE}/services/cassandra/nodetool flush
du -hd 0 ${PACKAGE}/db/cassandra/data
