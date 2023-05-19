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
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.complex.LargeDataTest",
    srcs = ["LargeDataTest.java"],
    data = [
        "//test/benchmark/reasoner/iam/resources:common",
    ],
    native_libraries_deps = ["//test/benchmark/reasoner/iam/common:benchmark-lib"],
)

host_compatible_java_test(
    name = "complex-rule-graph-test",
    size = "large",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.complex.ComplexRuleGraphTest",
    srcs = ["ComplexRuleGraphTest.java"],
    data = [
        "//test/benchmark/reasoner/iam/resources:common",
        "complex-rule-graph-test.tql"
    ],
    native_libraries_deps = ["//test/benchmark/reasoner/iam/common:benchmark-lib"],
)

host_compatible_java_test(
    name = "conjunction-structure-test",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.complex.ConjunctionStructureTest",
    srcs = ["ConjunctionStructureTest.java"],
    data = [
        "//test/benchmark/reasoner/iam/resources:common",
        "conjunction-structure-test.tql",
    ],
    native_libraries_deps = ["//test/benchmark/reasoner/iam/common:benchmark-lib"],
)

host_compatible_java_test(
    name = "language-feature-test",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.complex.LanguageFeatureTest",
    srcs = ["LanguageFeatureTest.java"],
    data = [
        "//test/benchmark/reasoner/iam/resources:common",
        "language-features-test.tql",
    ],
    native_libraries_deps = ["//test/benchmark/reasoner/iam/common:benchmark-lib"],
)

host_compatible_java_test(
    name = "real-queries-test",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.complex.RealQueriesTest",
    srcs = ["RealQueriesTest.java"],
    data = [
        "//test/benchmark/reasoner/iam/resources:common",
    ],
    native_libraries_deps = ["//test/benchmark/reasoner/iam/common:benchmark-lib"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl-header",
)
