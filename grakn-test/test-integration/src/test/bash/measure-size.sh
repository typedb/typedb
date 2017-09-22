#!/usr/bin/env bash

source env.sh

nodetool flush
du -hd 0 ${PACKAGE}/db/cassandra/data
