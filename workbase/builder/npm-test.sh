#!/usr/bin/env bash

export PATH=`pwd`/external/nodejs/bin:$PATH
export NODE_PATH=`pwd`/workbase/node_modules/

pushd workbase
npm $*
popd
