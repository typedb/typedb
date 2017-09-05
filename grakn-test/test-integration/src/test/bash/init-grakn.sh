#!/usr/bin/env bash

# force script to exit on failed command
set -e

if [ -d grakn-package ] ;  then rm -rf grakn-package ; fi

mkdir grakn-package

tar -xf grakn-dist/target/grakn-dist*.tar.gz --strip=1 -C grakn-package

grakn.sh start
