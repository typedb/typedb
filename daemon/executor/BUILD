#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
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

package(default_visibility = ["//daemon:__pkg__"])

load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "grakn-daemon-executor",
    srcs = glob(["**/*.java"]),
    deps = [
        # Package dependencies
        "//daemon/exception",

        # Grakn Core dependencies
        "//common",
        "//server",

        # External dependencies
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-core", # NEEDS TO GO
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-databind", # NEEDS TO GO
        "//dependencies/maven/artifacts/com/fasterxml/jackson/dataformat:jackson-dataformat-yaml", # NEEDS TO GO
        "//dependencies/maven/artifacts/com/google/auto/value:auto-value",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all", # NEEDS TO GO
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
    ]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":grakn-daemon-executor"
    ],
)