#!/usr/bin/env bash

source env.sh

nodetool flush
du -hd 0 grakn-package/db/cassandra/data
