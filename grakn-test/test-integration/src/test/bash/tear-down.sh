#!/usr/bin/env bash

source env.sh

if [ -d maven ] ;  then rm -rf maven ; fi
grakn-package/bin/grakn.sh stop
if [ -d grakn-package ] ;  then rm -rf grakn-package ; fi
