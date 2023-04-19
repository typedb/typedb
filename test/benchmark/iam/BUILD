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
    name = "large-data-test",
    size = "large",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.LargeDataTest",
    srcs = ["LargeDataTest.java",
            "Benchmark.java",
    ],
    data = [":resources/data.tql", ":resources/schema_types.tql", ":resources/schema_rules.tql"],
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
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",

        # External dependencies from Maven
        "@maven//:com_eclipsesource_minimal_json_minimal_json",
    ],
)

host_compatible_java_test(
    name = "complex-rule-graph-test",
    size = "large",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.ComplexRuleGraphTest",
    srcs = ["ComplexRuleGraphTest.java",
            "Benchmark.java",
    ],
    data = [":resources/data.tql", ":resources/schema_types.tql", ":resources/schema_rules.tql"],
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
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",
    ],
)

host_compatible_java_test(
    name = "complex-conjunctions-test",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.ComplexConjunctionsTest",
    srcs = ["ComplexConjunctionsTest.java",
            "Benchmark.java",
    ],
    data = [":resources/data.tql", ":resources/schema_types.tql", ":resources/schema_rules.tql"],
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
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*"]),
    license_type = "agpl-header",
)
