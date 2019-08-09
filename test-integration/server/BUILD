#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2019 Grakn Labs Ltd
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
    name = "attribute-uniqueness-it",
    test_class = "grakn.core.server.AttributeUniquenessIT",
    srcs = ["AttributeUniquenessIT.java"],
    deps = [
        "//concept:concept",
        "//common",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",

        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "grakn-client-it",
    srcs = ["GraknClientIT.java"],
    test_class = "grakn.core.server.GraknClientIT",
    deps = [
        # Package dependencies
        "@graknlabs_client_java//:client-java",

        # Internal dependencies
        "//api:api",
        "//concept:concept",
        "//server:server", # TODO: NEEDS TO GO
        "//test-integration/rule:grakn-test-server",

        # External depencies from @graknlabs
        "@graknlabs_graql//java:graql",

        # External depencncies from Maven
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
    classpath_resources = [
        "//test-integration/resources:logback-test",
        "//test-integration/resources:cassandra-embedded",
        "//server:conf/grakn.properties"
    ]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":attribute-uniqueness-it",
        ":grakn-client-it"
    ],
)