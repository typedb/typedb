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


package(default_visibility = ["//visibility:__subpackages__"])
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "test",
    srcs = [
        "RuleValidationTest.java",
    ],
    test_class = "grakn.core.test.behaviour.graql.language.rule_validation.RuleValidationTest",
    deps = [
        # Package dependencies
        "//test/behaviour/server:server",

        # External dependencies from Maven
        "@maven//:io_cucumber_cucumber_junit",
    ],
    runtime_deps = [
        "//test/behaviour/graql:steps",
        "//test/behaviour/connection:steps",
        "//test/behaviour/connection/session:steps",
        "//test/behaviour/config:parameters",
    ],
    data = [
        "@graknlabs_verification//behaviour/graql/language:rule-validation.feature",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    size = "medium",
    visibility = ["//visibility:public"]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":test",
    ],
)
