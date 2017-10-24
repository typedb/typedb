#!/usr/bin/env bash

set -e

if [ -d grakn-new ]; then
    rm -r grakn-new
fi

mkdir grakn-new

tar -xf grakn-dist*.tar.gz --strip=1 -C grakn-new

if [ -d grakn ]; then
    grakn/grakn server stop
    rm -r grakn
fi

mv grakn-new grakn
grakn/grakn server start

grakn/graql console -k pokemon -f grakn/examples/pokemon.gql

sudo systemctl restart repeat-query