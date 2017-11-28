#!/usr/bin/env bash

source env.sh

# this a bash trick to specify the default value
args=${@:-arch validate}

download-snb.sh
load-SNB.sh ${args}
measure-size.sh