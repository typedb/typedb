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
load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")


java_test(
    name = "grakn-client-test",
    srcs = [
        "GraknClientTest.java",
        "GraknServerRPCMock.java",
    ],
    test_class = "grakn.core.client.test.GraknClientTest",
    deps = [
        # Package dependencies
        "//client-java",

        # Grakn Core dependencies
        "//common:common",
        "//protocol:protocol-java",
        "@graknlabs_graql//java:graql",
        "//server:server", # NEEDS TO GO

        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-testing",
        "//dependencies/maven/artifacts/io/grpc:grpc-stub",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/com/google/guava:guava",
    ],
    size = "small",
)

java_test(
    name = "grakn-client-it",
    srcs = ["GraknClientIT.java"],
    test_class = "grakn.core.client.test.GraknClientIT",
    deps = [
        # Package dependencies
        "//client-java",

        # Grakn Core dependencies
        "@graknlabs_graql//java:graql",
        "//server:server", # TODO: NEEDS TO GO
        "//test-integration/rule:grakn-test-server",

        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":grakn-client-test",
        ":grakn-client-it",
    ],
)
