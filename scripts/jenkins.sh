#!/usr/bin/env bash

source scripts/env.sh

${WORKSPACE}/scripts/load.sh $@
${WORKSPACE}/scripts/validate.sh $@
