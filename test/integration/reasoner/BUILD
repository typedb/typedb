#
# Copyright (C) 2021 Vaticle
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
        "//rocks:rocks",
        "//logic:logic",
        "//:typedb",
        "//concept:concept",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",
        "//common:common",
        "//concurrent:concurrent",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
    resources = [
        "//common/test:logback-test"
    ],
    resource_strip_prefix = "common/test",
)

host_compatible_java_test(
    name = "test-explanation",
    srcs = ["ExplanationTest.java"],
    test_class = "com.vaticle.typedb.core.reasoner.ExplanationTest",
    native_libraries_deps = [
        "//concept:concept",
        "//logic:logic",
        "//reasoner:reasoner",
        "//rocks:rocks",
        "//traversal:traversal",
        "//:typedb",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util:util",
        "//common:common",
        "//concurrent:concurrent",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//:typeql-lang",
    ],
    resources = [
        "//common/test:logback-test"
    ],
    resource_strip_prefix = "common/test",
)

host_compatible_java_test(
    name = "test-resolution",
    srcs = ["resolution/ResolutionTest.java", "resolution/Util.java"],
    test_class = "com.vaticle.typedb.core.reasoner.resolution.ResolutionTest",
    native_libraries_deps = [
        "//concept:concept",
        "//logic:logic",
        "//pattern:pattern",
        "//reasoner:reasoner",
        "//rocks:rocks",
        "//traversal:traversal",
        "//:typedb",
    ],
    deps = [
        # Internal dependencies
        "//common:common",
        "//concurrent:concurrent",
        "//test/integration/util",

        # External dependencies from Vaticle
        "@vaticle_typeql_lang_java//:typeql-lang",
        "@vaticle_typedb_common//:common",
    ],
    resource_strip_prefix = "common/test",
    resources = [
        "//common/test:logback-test",
    ],
)

host_compatible_java_test(
    name = "test-resolution-planner",
    srcs = ["resolution/PlannerTest.java", "resolution/Util.java"],
    test_class = "com.vaticle.typedb.core.reasoner.resolution.PlannerTest",
    native_libraries_deps = [
        "//rocks:rocks",
        "//:typedb",
        "//concept:concept",
        "//pattern:pattern",
        "//logic:logic",
        "//reasoner:reasoner",
    ],
    deps = [
        # Internal dependencies
        "//test/integration/util",
        "//common",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//:typeql-lang",
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
