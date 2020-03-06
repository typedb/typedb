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

load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "attribute-uniqueness-it",
    size = "large",
    srcs = ["AttributeUniquenessIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.AttributeUniquenessIT",
    deps = [
        "//concept/answer",
        "//core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
        "@graknlabs_common//:common",
    ],
)

java_test(
    name = "grakn-client-it",
    size = "large",
    srcs = ["GraknClientIT.java"],
    classpath_resources = [
        "//test-integration/resources:logback-test",
        "//test-integration/resources:cassandra-embedded",
        "//server:conf/grakn.properties",
    ],
    test_class = "grakn.core.server.GraknClientIT",
    deps = [
        # Package dependencies
        "@graknlabs_client_java//:client-java",

        # Internal dependencies
        "//kb/server",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",

        # External depencies from @graknlabs
        "@graknlabs_graql//java:graql",

        # External depencncies from Maven
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/google/guava",
    ],
)



java_test(
    name = "validator-it",
    size = "medium",
    srcs = ["ValidatorIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.ValidatorIT",
    deps = [
        "//common:common",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "validate-global-rules-it",
    size = "medium",
    srcs = ["ValidateGlobalRulesIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.ValidateGlobalRulesIT",
    deps = [
        "//concept/impl:impl",
        "//kb/server",
        "//kb/concept/api",
        "//kb/concept/structure",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:concept-downcasting",
    ],
)

java_test(
    name = "rule-validation-it",
    size = "large",
    srcs = ["RuleValidationIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.RuleValidationIT",
    deps = [
        "//common:common",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//graql/reasoner",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)


checkstyle_test(
    name = "checkstyle",
    targets = [
        ":attribute-uniqueness-it",
        ":grakn-client-it",
        ":validator-it",
        ":validate-global-rules-it",
        ":rule-validation-it",
    ],
)
