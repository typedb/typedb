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

package(default_visibility = ["//visibility:__pkg__"])

java_test(
    name = "remote-concept-it",
    srcs = ["RemoteConceptIT.java"],
    test_class = "grakn.core.client.concept.test.RemoteConceptIT",
    deps = [
        # Grakn Core dependencies
        "//client-java",
        "//server", # NEEDS TO GO

        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)


load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")
checkstyle_test(
    name = "remote-concept-it-checkstyle",
    target = ":remote-concept-it",
    config = "//config/checkstyle:checkstyle.xml",
    suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
    license = "//config/checkstyle:checkstyle-file-header.txt",
)
