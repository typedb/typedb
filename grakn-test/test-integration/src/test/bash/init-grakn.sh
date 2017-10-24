#!/usr/bin/env bash

source env.sh

if [ -d "${PACKAGE}" ] ;  then rm -rf ${PACKAGE} ; fi

mkdir ${PACKAGE}

tar -xf grakn-dist/target/grakn-dist*.tar.gz --strip=1 -C ${PACKAGE}

# set DEBUG log level
# TODO: support this in a more elegant way (command line arg?)
sed -i'' -e 's/log.level=INFO/log.level=DEBUG/g' "${PACKAGE}/conf/grakn.properties"

grakn server start
