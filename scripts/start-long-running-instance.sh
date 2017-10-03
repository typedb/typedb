#!/usr/bin/env bash

set -e

mkdir grakn-new -p

tar -xf grakn-dist*.tar.gz --strip=1 -C grakn-new

if [ -d grakn ]; then
    grakn/grakn server stop
    rm -r grakn
fi

mv grakn-new grakn
grakn/grakn server start
