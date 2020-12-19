#!/usr/bin/env bash

popd > /dev/null

[[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
OUT_DIR=$(cd "$(dirname "${path}")" && pwd -P)
pushd "$OUT_DIR" > /dev/null

bazel query "filter('^(?!(//dependencies|@graknlabs|//test|//common).*$).*', kind(java_library, deps($1)))" --output graph > "$2".dot
dot -Tpng < "$2".dot > "$2".png
open "$2".png

popd > /dev/null
