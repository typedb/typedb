#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Script for updating Maven dependencies after the dependency list in //dependencies/maven/dependencies.yaml.

[[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
CLIENT_JAVA_HOME=$(cd "$(dirname "${path}")" && pwd -P)/../../
pushd "$CLIENT_JAVA_HOME" > /dev/null

bazel run @graknlabs_build_tools//bazel:bazel-deps -- generate -r $CLIENT_JAVA_HOME -s dependencies/maven/dependencies.bzl -d dependencies/maven/dependencies.yaml

# Fix formatting for Bazel source code
#bazel run //tools/formatter -- --path $(pwd)/third_party --build &>/dev/null

popd > /dev/null