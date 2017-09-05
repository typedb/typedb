#!/usr/bin/env bash

# force script to exit on failed command
set -e

if [ -d maven ] ;  then rm -rf maven ; fi
grakn-package/bin/grakn.sh stop
if [ -d grakn-package ] ;  then rm -rf grakn-package ; fi
