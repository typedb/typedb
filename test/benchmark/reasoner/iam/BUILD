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
    name = "basic-test",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.BasicTest",
    srcs = ["BasicTest.java",
            "BenchmarkRunner.java",
            "Benchmark.java",
            "QueryParams.java",
    ],
    data = [
        ":resources/query_params.yml",
        ":resources/data.typedb",
        ":resources/schema_types.tql",
        ":resources/schema_rules_basic_test.tql",
    ],
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//common:common",
        "//concurrent:concurrent",
        "//migrator:migrator",
        "//reasoner:reasoner",
    ],
    deps = [
        "//server:version",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",

        # External dependencies from Maven
        "@maven//:org_slf4j_slf4j_api"
    ]
)

host_compatible_java_test(
    name = "large-data-test",
    size = "large",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.LargeDataTest",
    srcs = ["LargeDataTest.java",
            "BenchmarkRunner.java",
            "Benchmark.java",
            "QueryParams.java",
    ],
    data = [
        ":resources/query_params.yml",
        ":resources/data.typedb",
        ":resources/schema_types.tql",
        ":resources/schema_rules_optimised.tql",
    ],
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//common:common",
        "//concurrent:concurrent",
        "//migrator:migrator",
        "//reasoner:reasoner",
    ],
    deps = [
        "//server:version",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
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
            "BenchmarkRunner.java",
            "Benchmark.java",
            "QueryParams.java",
    ],
    data = [
        ":resources/query_params.yml",
        ":resources/data.typedb",
        ":resources/schema_types.tql",
        ":resources/schema_rules_naive.tql"
    ],
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//common:common",
        "//concurrent:concurrent",
        "//migrator:migrator",
        "//reasoner:reasoner",
    ],
    deps = [
        "//server:version",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",

        # External dependencies from Maven
        "@maven//:org_slf4j_slf4j_api"
    ]
)

host_compatible_java_test(
    name = "conjunction-structure-test",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.ConjunctionStructureTest",
    srcs = ["ConjunctionStructureTest.java",
            "BenchmarkRunner.java",
            "Benchmark.java",
            "QueryParams.java",
    ],
    data = [
        ":resources/query_params.yml",
        ":resources/data.typedb",
        ":resources/schema_types.tql",
        ":resources/schema_rules_optimised.tql",
        ":resources/schema_rules_test_specific.tql",
    ],
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//common:common",
        "//concurrent:concurrent",
        "//migrator:migrator",
        "//reasoner:reasoner",
    ],
    deps = [
        "//server:version",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",

        # External dependencies from Maven
        "@maven//:org_slf4j_slf4j_api"
    ]
)

host_compatible_java_test(
    name = "language-feature-test",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.LanguageFeatureTest",
    srcs = ["LanguageFeatureTest.java",
            "BenchmarkRunner.java",
            "Benchmark.java",
            "QueryParams.java",
    ],
    data = [
        ":resources/query_params.yml",
        ":resources/data.typedb",
        ":resources/schema_types.tql",
        ":resources/schema_rules_optimised.tql",
        ":resources/schema_rules_test_specific.tql",
    ],
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//common:common",
        "//concurrent:concurrent",
        "//migrator:migrator",
        "//reasoner:reasoner",
    ],
    deps = [
        "//server:version",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",

        # External dependencies from Maven
        "@maven//:org_slf4j_slf4j_api"
    ]
)

host_compatible_java_test(
    name = "real-queries-test",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.RealQueriesTest",
    srcs = ["RealQueriesTest.java",
            "BenchmarkRunner.java",
            "Benchmark.java",
            "QueryParams.java",
    ],
    data = [
        ":resources/query_params.yml",
        ":resources/data.typedb",
        ":resources/schema_types.tql",
        ":resources/schema_rules_optimised.tql"
    ],
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//common:common",
        "//concurrent:concurrent",
        "//migrator:migrator",
        "//reasoner:reasoner",
    ],
    deps = [
        "//server:version",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",

        # External dependencies from Maven
        "@maven//:org_slf4j_slf4j_api"
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*"]),
    exclude = ["resources/data.typedb", "resources/data_large.typedb"],
    license_type = "agpl-header",
)
