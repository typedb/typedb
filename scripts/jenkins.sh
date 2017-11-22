#!/usr/bin/env bash

./env.sh

build-grakn.sh &&
init-grakn.sh &&
load.sh &&
validate.sh;

echo "Tearing down"
tear-down.sh