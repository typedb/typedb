#
# Copyright (C) 2021 Grakn Labs
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

load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@graknlabs_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

host_compatible_java_test(
    name = "test-reasoner",
    srcs = ["ReasonerTest.java"],
    test_class = "grakn.core.reasoner.ReasonerTest",
    native_libraries_deps = [
        "//rocks:rocks",
        "//logic:logic",
        "//:grakn",
        "//concept:concept",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",
        "//common:common",
        "//concurrent:concurrent",

        # External dependencies from Grakn Labs
        "@graknlabs_graql//java:graql",
    ],
    resources = [
        "//common/test:logback"
    ],
    resource_strip_prefix = "common/test",
)

host_compatible_java_test(
    name = "test-resolution",
    srcs = ["ResolutionTest.java"],
    native_libraries_deps = [
        "//concept:concept",
        "//logic:logic",
        "//pattern:pattern",
        "//reasoner:reasoner",
        "//rocks:rocks",
        "//:grakn",
    ],
    resource_strip_prefix = "common/test",
    resources = [
        "//common/test:logback",
    ],
    test_class = "grakn.core.reasoner.ResolutionTest",
    deps = [
        # Internal dependencies
        "//common:common",
        "//concurrent:concurrent",
        "//test/integration/util",
        "@graknlabs_graql//java:graql",
    ],
)

host_compatible_java_test(
    name = "test-resolution-planner",
    srcs = ["resolution/PlannerTest.java"],
    native_libraries_deps = [
        "//rocks:rocks",
        "//:grakn",
        "//concept:concept",
        "//pattern:pattern",
        "//logic:logic",
        "//reasoner:reasoner",
    ],
    test_class = "grakn.core.reasoner.resolution.PlannerTest",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        "//common",

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "resolution/*",
    ]),
    license_type = "agpl",
)
