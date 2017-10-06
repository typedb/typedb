#!/usr/bin/env bash

source env.sh

if [ -d maven ] ;  then rm -rf maven ; fi
grakn server stop
if [ -d ${PACKAGE} ] ;  then rm -rf ${PACKAGE} ; fi
