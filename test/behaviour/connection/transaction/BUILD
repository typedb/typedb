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


package(default_visibility = ["//test/behaviour:__subpackages__"])
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "steps",
    srcs = [
        "TransactionSteps.java"
    ],
    deps = [
        # Internal Package Dependencies
        "//kb/server",
        "//test/behaviour/connection:steps",

        # Internal Repository Dependencies
        "@graknlabs_common//:common",

        # External Maven Dependencies
        "//dependencies/maven/artifacts/junit:junit",
        "//dependencies/maven/artifacts/io/cucumber:cucumber-java",
    ]
)

java_test(
    name = "test",
    srcs = [
        "TransactionTest.java",
    ],
    test_class = "grakn.core.test.behaviour.connection.transaction.TransactionTest",
    deps = [
        # Package dependencies
        "//test/behaviour/server:server",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/io/cucumber:cucumber-junit",
    ],
    runtime_deps = [
        ":steps",
        "//test/behaviour/config:parameters",
        "//test/behaviour/connection/keyspace:steps",
        "//test/behaviour/connection/session:steps",
    ],
    data = [
        "@graknlabs_verification//behaviour/connection:transaction.feature",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    size = "large",
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":steps",
        ":test",
    ],
)
