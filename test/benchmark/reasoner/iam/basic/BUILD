# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test", "native_java_libraries")

host_compatible_java_test(
    name = "test-basic",
    test_class = "com.vaticle.typedb.core.reasoner.benchmark.iam.basic.BasicTest",
    srcs = ["BasicTest.java"],
    data = [
        "//test/benchmark/reasoner/iam/resources:common",
        "basic_test.tql",
    ],
    native_libraries_deps = [
        "//test/benchmark/reasoner/iam/common:common",
        "//common:common",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob(["*.typedb"]),
    license_type = "mpl-header",
)
