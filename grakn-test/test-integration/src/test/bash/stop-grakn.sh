#!/usr/bin/env bash

source env.sh

grakn server stop

if [ -d ${PACKAGE} ] ;  then rm -rf ${PACKAGE} ; fi

