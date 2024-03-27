# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test", "native_java_libraries")

host_compatible_java_test(
    name = "test-large-data",
    size = "large",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.complex.LargeDataTest",
    srcs = ["LargeDataTest.java"],
    data = [
        "//test/benchmark/reasoner/iam/resources:common",
    ],
    native_libraries_deps = [
        "//test/benchmark/reasoner/iam/common:common",
        "//common:common",
    ],
)

host_compatible_java_test(
    name = "test-complex-rule-graph",
    size = "large",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.complex.ComplexRuleGraphTest",
    srcs = ["ComplexRuleGraphTest.java"],
    data = [
        "//test/benchmark/reasoner/iam/resources:common",
        "complex-rule-graph-test.tql"
    ],
    native_libraries_deps = [
        "//test/benchmark/reasoner/iam/common:common",
        "//common:common",
    ],
)

host_compatible_java_test(
    name = "test-conjunction-structure",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.complex.ConjunctionStructureTest",
    srcs = ["ConjunctionStructureTest.java"],
    data = [
        "//test/benchmark/reasoner/iam/resources:common",
        "conjunction-structure-test.tql",
    ],
    native_libraries_deps = [
        "//test/benchmark/reasoner/iam/common:common",
        "//common:common",
    ],
)

host_compatible_java_test(
    name = "test-language-features",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.complex.LanguageFeaturesTest",
    srcs = ["LanguageFeaturesTest.java"],
    data = [
        "//test/benchmark/reasoner/iam/resources:common",
        "language-features-test.tql",
    ],
    native_libraries_deps = [
        "//test/benchmark/reasoner/iam/common:common",
        "//common:common",
    ],
)

host_compatible_java_test(
    name = "test-real-queries",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.complex.RealQueriesTest",
    srcs = ["RealQueriesTest.java"],
    data = [
        "//test/benchmark/reasoner/iam/resources:common",
    ],
    native_libraries_deps = [
        "//test/benchmark/reasoner/iam/common:common",
        "//common:common",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
