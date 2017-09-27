#!/usr/bin/env bash

source env.sh

if [ -d "${PACKAGE}" ] ;  then rm -rf ${PACKAGE} ; fi

mkdir ${PACKAGE}

tar -xf grakn-dist/target/grakn-dist*.tar.gz --strip=1 -C ${PACKAGE}

grakn server start
