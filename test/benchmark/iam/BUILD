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
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test", "native_java_libraries")

host_compatible_java_test(
    name = "large-data-test",
    size = "large",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.LargeDataTest",
    srcs = ["LargeDataTest.java",
            "ReasonerBenchmarkSuite.java",
            "BenchmarkRunner.java",
            "Benchmark.java",
    ],
    data = [
        ":resources/data_small.typedb",
        ":resources/schema_types.tql",
        ":resources/schema_rules_optimised.tql"
    ],
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//concept:concept",
        "//common:common",
        "//concurrent:concurrent",
        "//logic:logic",
        "//migrator:migrator",
        "//reasoner:reasoner",
        "//traversal:traversal",
    ],
    deps = [
        "//server:version",

        # External dependencies from Vaticle
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",

        # External dependencies from Maven
        "@maven//:org_slf4j_slf4j_api"
    ]
)

host_compatible_java_test(
    name = "complex-rule-graph-test",
    size = "large",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.ComplexRuleGraphTest",
    srcs = ["ComplexRuleGraphTest.java",
            "ReasonerBenchmarkSuite.java",
            "BenchmarkRunner.java",
            "Benchmark.java",
    ],
    data = [
        ":resources/data_small.typedb",
        ":resources/schema_types.tql",
        ":resources/schema_rules_naive.tql"
    ],
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//concept:concept",
        "//common:common",
        "//concurrent:concurrent",
        "//logic:logic",
        "//migrator:migrator",
        "//reasoner:reasoner",
        "//traversal:traversal",
    ],
    deps = [
        "//server:version",

        # External dependencies from Vaticle
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",

        # External dependencies from Maven
        "@maven//:org_slf4j_slf4j_api"
    ]
)

host_compatible_java_test(
    name = "complex-conjunctions-test",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.ComplexConjunctionTest",
    srcs = ["ComplexConjunctionTest.java",
            "ReasonerBenchmarkSuite.java",
            "BenchmarkRunner.java",
            "Benchmark.java",
    ],
    data = [
        ":resources/data_small.typedb",
        ":resources/schema_types.tql",
        ":resources/schema_rules_optimised.tql"
    ],
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//concept:concept",
        "//common:common",
        "//concurrent:concurrent",
        "//logic:logic",
        "//migrator:migrator",
        "//reasoner:reasoner",
        "//traversal:traversal",
    ],
    deps = [
        "//server:version",

        # External dependencies from Vaticle
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",

        # External dependencies from Maven
        "@maven//:org_slf4j_slf4j_api"
    ]
)


native_java_libraries(
    name = "all-benchmarks",
    srcs = glob(["*.java"], exclude=["Runner.java"]),
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//concept:concept",
        "//common:common",
        "//concurrent:concurrent",
        "//logic:logic",
        "//migrator:migrator",
        "//reasoner:reasoner",
        "//traversal:traversal",
    ],
    deps = [
        "//server:version",

        # External dependencies from Vaticle
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",

        # External dependencies from Maven
        "@maven//:junit_junit",
        "@maven//:org_slf4j_slf4j_api"
    ]
)

java_binary(
    name = "run-mac",
    main_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.Runner",
    srcs = ["Runner.java"],
    data = glob(["resources/*"]),
    deps = [
        ":all-benchmarks-mac",

        "@maven//:junit_junit",
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:ch_qos_logback_logback_classic"
    ],
)

java_binary(
    name = "run-linux",
    main_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.Runner",
    srcs = ["Runner.java"],
    data = glob(["resources/*"]),
    deps = [
        ":all-benchmarks-linux",

        "@maven//:junit_junit",
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:ch_qos_logback_logback_classic"
    ]
)

java_binary(
    name = "run-windows",
    main_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.Runner",
    srcs = ["Runner.java"],
    data = glob(["resources/*"]),
    deps = [
        ":all-benchmarks-windows",

        "@maven//:junit_junit",
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:ch_qos_logback_logback_classic"
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*"], exclude=["resources/data_small.typedb"]),
    license_type = "agpl-header",
)
