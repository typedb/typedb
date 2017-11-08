#!/usr/bin/env bash

source env.sh

if [ -d maven ] ;  then rm -rf maven ; fi

if type grakn ; then grakn server stop ; fi

if [ -d ${PACKAGE} ] ;  then rm -rf ${PACKAGE} ; fi

if pgrep -l redis-server ; then
    echo "WARNING: Redis is still running at tear down - killing process"
    pkill -9 redis-server
fi
