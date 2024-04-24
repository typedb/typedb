#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

# RUN COMMAND: ./docs/deps.sh //... package-structure

popd > /dev/null

[[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
OUT_DIR=$(cd "$(dirname "${path}")" && pwd -P)
pushd "$OUT_DIR" > /dev/null

bazel query "filter('^(?!(//dependencies|@vaticle|//test|//common).*$).*', kind(java_library, deps($1)))" --output graph > "$2".dot
dot -Tpng < "$2".dot > "$2".png
open "$2".png

popd > /dev/null
