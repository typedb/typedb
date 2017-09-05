#!/usr/bin/env bash

# force script to exit on failed command
set -e

nodetool flush
du -hd 0 grakn-package/db/cassandra/data
