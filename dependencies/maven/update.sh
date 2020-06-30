#!/usr/bin/env bash
#
# Copyright (C) 2020 Grakn Labs
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

[[ $(readlink $0) ]] && path=$(readlink $0) || path=$0
GRAKN_CORE_HOME=$(cd "$(dirname "${path}")" && pwd -P)/../../
pushd "$GRAKN_CORE_HOME" > /dev/null

bazel run @maven//:pin
mv maven_install.json dependencies/maven/snapshot.json

popd > /dev/null
