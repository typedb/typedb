#!/usr/bin/env bash

source env.sh

# this a bash trick to specify the default value
args=${@:-arch validate}

load-SNB.sh ${args}
measure-size.sh