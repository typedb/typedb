#!/usr/bin/env bash

source env.sh

if [ "$#" -gt "0" ]; then
    echo "Using branch name ${1}"
    DIST=grakn-dist/target/grakn-dist-${1}.tar.gz
else
    # get first thing matching wildcard pattern
    DIST=(grakn-dist/target/grakn-dist*.tar.gz)
    DIST=${DIST[0]}
fi

if [ -d "${PACKAGE}" ] ;  then rm -rf ${PACKAGE} ; fi

mkdir ${PACKAGE}

tar -xf ${DIST} --strip=1 -C ${PACKAGE}

# set DEBUG log level
# TODO: support this in a more elegant way (command line arg?)
sed -i'' -e 's/log.level=INFO/log.level=DEBUG/g' "${PACKAGE}/conf/grakn.properties"

grakn server start
