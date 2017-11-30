#!/usr/bin/env bash

source scripts/env.sh

build-grakn.sh
init-grakn.sh
time load.sh ${@:2}