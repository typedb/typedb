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

load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

package(
    default_visibility = [
        "//:__pkg__",
        "//cluster:__pkg__",
        "//rocks:__pkg__",
        "//test:__subpackages__",
    ],
)

java_library(
    name = "query",
    srcs = glob(["*.java"]),
    deps = [
        # Internal package dependencies
        "//common:common",
        "//concept:concept",
        "//graph:graph",

        # External dependencies from @graknlabs
        "@graknlabs_grabl_tracing//client:client",
        "@graknlabs_graql//java:graql",
    ],
    tags = ["maven_coordinates=grakn.core:grakn-core-query:{pom_version}"],
    visibility = [ "//visibility:public" ]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":query",
    ],
)
