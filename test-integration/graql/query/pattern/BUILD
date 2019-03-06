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

load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "pattern-it",
    test_class = "grakn.core.graql.query.pattern.PatternIT",
    srcs = ["PatternIT.java"],
    deps = [
        "//concept:concept",
        "//server:server",
        "//test-integration/graql/graph:movie-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",

        "@graknlabs_graql//java:graql",

        "//dependencies/maven/artifacts/com/google/guava:guava",

    ],
    size = "medium"
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":pattern-it"
    ],
)