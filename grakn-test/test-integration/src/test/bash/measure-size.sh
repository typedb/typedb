#!/usr/bin/env bash

source env.sh

${PACKAGE}/services/cassandra/nodetool flush
# `|| true` means we ignore if this command fails (it can if e.g. files are in the process of being deleted)
du -hd 0 ${PACKAGE}/db/cassandra/data || true
