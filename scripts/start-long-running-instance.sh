#!/usr/bin/env bash

set -e

if [ -d grakn ]; then
    grakn/grakn server stop
    rm -r grakn
fi

mv grakn-new grakn
grakn/grakn server start
