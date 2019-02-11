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

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "parser-test",
    test_class = "grakn.core.graql.parser.ParserTest",
    srcs = ["ParserTest.java"],
    deps = [
        "//common:common",
        "//graql/java:graql",
        "//server:server",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
    ],
    size = "medium"
)

java_test(
    name = "query-to-string-test",
    test_class = "grakn.core.graql.parser.QueryToStringTest",
    srcs = ["QueryToStringTest.java"],
    deps = [
        "//graql/java:graql",
        "//server:server",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
    ],
    size = "small"
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":parser-test",
        ":query-to-string-test"
    ]
)
