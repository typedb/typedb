#!/usr/bin/env bash
#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2019 Grakn Labs Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

# Script for updating Maven dependencies after the dependency list in //dependencies/maven/dependencies.yaml.

[[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
GRAKN_CORE_HOME=$(cd "$(dirname "${path}")" && pwd -P)/../../
pushd "$GRAKN_CORE_HOME" > /dev/null

bazel run @graknlabs_build_tools//bazel:bazel-deps -- generate -r $GRAKN_CORE_HOME -s dependencies/maven/dependencies.bzl -d dependencies/maven/dependencies.yaml

# Fix formatting for Bazel source code
#bazel run //tools/formatter -- --path $(pwd)/third_party --build &>/dev/null

popd > /dev/null