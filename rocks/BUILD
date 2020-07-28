#
# Copyright (C) 2019 Grakn Labs
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
        "//server:__pkg__",
        "//test:__subpackages__",
    ]
)

java_library(
    name = "rocks",
    srcs = glob(["*.java"]),
    deps = [
        # Internal package dependencies
        "//:grakn",
        "//common:common",
        "//concept:concept",
        "//graph:graph",
        "//query:query",

        # External dependencies from @graknlabs
        "@graknlabs_common//:common",

        # External dependencies
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:org_rocksdb_rocksdbjni",
    ],
    tags = ["maven_coordinates=grakn.core:grakn-core-rocks:{pom_version}"],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":rocks",
    ]
)
