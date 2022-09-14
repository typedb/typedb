#!/usr/bin/env bash

set -e
rm -rf dist/
bazel build //:assemble-mac-zip
unzip ./bazel-bin/typedb-all-mac.zip -d ./dist/
mkdir -p ./dist/typedb-all-mac/server/data
#cp -v -R ./data/biograkn-semmed ./dist/typedb-all-mac/server/data
export SERVER_JAVAOPTS="-Xmx16G"
./dist/typedb-all-mac/typedb server
