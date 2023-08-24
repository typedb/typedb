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
    name = "benchmark-small",
    size = "large",
    srcs = [
        "BenchmarkSmall.java",
        "Util.java",
        "generation/DiagonalGraph.java",
        "generation/PathTreeGraph.java",
        "generation/TransitivityChainGraph.java",
        "generation/TransitivityMatrixGraph.java",
    ],
    data = [
        ":resources/diagonalTest.tql",
        ":resources/linearTransitivity.tql",
        ":resources/pathTest.tql",
        ":resources/quadraticTransitivity.tql",
    ],
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
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.synthetic.BenchmarkSmall",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        # External dependencies from Vaticle
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",
    ],
)

host_compatible_java_test(
    name = "benchmark-big",
    size = "large",
    srcs = [
        "BenchmarkBig.java",
        "Util.java",
    ],
    data = [
        ":resources/linearTransitivity.tql",
        ":resources/multiJoin.tql",
    ],
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
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.synthetic.BenchmarkBig",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        # External dependencies from Vaticle
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/builder",
        "@vaticle_typeql//java/query",
        "@vaticle_typeql//java/pattern",
    ],
)

host_compatible_java_test(
    name = "rule-scaling",
    size = "large",
    srcs = [
        "RuleScaling.java",
        "Util.java",
    ],
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
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.synthetic.RuleScaling",
    deps = [
        # Internal dependencies
        "//test/integration/util",
        # External dependencies from Vaticle
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/pattern",
        "@vaticle_typeql//java/query",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "*/*",
    ]),
    license_type = "agpl-header",
)
