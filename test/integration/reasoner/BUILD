#
# Copyright (C) 2022 Vaticle
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

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

host_compatible_java_test(
    name = "test-reasoner",
    srcs = ["ReasonerTest.java"],
    test_class = "com.vaticle.typedb.core.reasoner.ReasonerTest",
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//common:common",
        "//concurrent:concurrent",
        "//logic:logic",
        "//concept:concept",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java:typeql-lang",
    ],
)

host_compatible_java_test(
    name = "test-explanation",
    srcs = ["ExplanationTest.java"],
    test_class = "com.vaticle.typedb.core.reasoner.ExplanationTest",
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//concept:concept",
        "//common:common",
        "//concurrent:concurrent",
        "//logic:logic",
        "//reasoner:reasoner",
        "//traversal:traversal",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java:typeql-lang",
    ],
)

host_compatible_java_test(
    name = "test-controller",
    srcs = ["controller/ControllerTest.java"],
    test_class = "com.vaticle.typedb.core.reasoner.controller.ControllerTest",
    native_libraries_deps = [
        # Internal dependencies
        "//:typedb",
        "//database:database",
        "//common:common",
        "//concept:concept",
        "//concurrent:concurrent",
        "//logic:logic",
        "//pattern:pattern",
        "//reasoner:reasoner",
        "//traversal:traversal",
    ],
    deps = [
        "//test/integration/util",

        # External dependencies from Vaticle
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typedb_common//:common",
    ],
)

host_compatible_java_test(
    name = "test-planner",
    srcs = ["common/PlannerTest.java"],
    test_class = "com.vaticle.typedb.core.reasoner.common.PlannerTest",
    native_libraries_deps = [
        # Internal dependencies
        "//database:database",
        "//:typedb",
        "//common:common",
        "//concept:concept",
        "//pattern:pattern",
        "//logic:logic",
        "//reasoner:reasoner",
    ],
    deps = [
        "//test/integration/util",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java:typeql-lang",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "controller/*",
        "common/*",
    ]),
    license_type = "agpl-header",
)
