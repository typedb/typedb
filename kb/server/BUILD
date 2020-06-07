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
#
package(default_visibility = ["//visibility:public"])

load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "server",
    srcs = glob([
        "*.java",
        "cache/*.java",
        "exception/*.java",
        "keyspace/*.java",
    ]),
    tags = ["maven_coordinates=io.grakn.core:grakn-kb-server:{pom_version}"],
    deps = [
        # Internal dependencies
        "//common",
        "//kb/concept/api",
        "//kb/concept/structure",
        "//kb/keyspace",
        "//concept/answer",

        # this is ONLY used for ConceptManager dependency...
        "//core",

        # External dependenicies from @graknlabs
        "@graknlabs_graql//java:graql",
        "@graknlabs_common//:common",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":server",
    ],
)
