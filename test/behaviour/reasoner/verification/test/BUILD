# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

host_compatible_java_test(
    name = "test-materialiser",
    size = "medium",
    srcs = [
        "MaterialiserTest.java",
    ],
    test_class = "com.vaticle.typedb.core.test.behaviour.reasoner.verification.test.MaterialiserTest",
    native_libraries_deps = [
        "//:typedb",
        "//common:common",
        "//concept:concept",
        "//logic:logic",
        "//database:database",
        "//test/behaviour/reasoner/verification:verification",
    ],
    deps = [
        "//test/integration/util:util",
        # External dependencies from @vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",
        "@vaticle_typeql//java/common",
    ],
)

host_compatible_java_test(
    name = "test-correctness-verifier",
    size = "large",
    srcs = [
        "CorrectnessVerifierTest.java",
    ],
    test_class = "com.vaticle.typedb.core.test.behaviour.reasoner.verification.test.CorrectnessVerifierTest",
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//common",
        "//common/test:util",
        "//test/behaviour/reasoner/verification:verification",
    ],
    deps = [
        "//test/integration/util",
        # External dependencies from @vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",
        "@vaticle_typeql//java/common",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
