#!/usr/bin/env bash

source env.sh

# TODO: it shouldn't be necessary to re-build grakn to make SNB work
build-grakn.sh

download-snb.sh
load-SNB.sh arch validate
measure-size.sh