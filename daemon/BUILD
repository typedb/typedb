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

package(default_visibility = ["//visibility:__pkg__"])

load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "daemon",
    srcs = glob(["**/*.java"]),
    deps = [

        # Grakn Core dependencies
        "//common:common",
        "//server:server",

        # External dependencies
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-core", # NEEDS TO GO
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-databind", # NEEDS TO GO
        "//dependencies/maven/artifacts/com/fasterxml/jackson/dataformat:jackson-dataformat-yaml", # NEEDS TO GO
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/ch/qos/logback:logback-core",
    ],
    resources = ["LICENSE"],
    resource_strip_prefix = "daemon",
    visibility = ["//visibility:public"],
    tags = ["maven_coordinates=io.grakn.core:grakn-daemon:{pom_version}"],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":daemon"
    ],
)