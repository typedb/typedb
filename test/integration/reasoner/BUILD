# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//builder/java:rules.bzl", "host_compatible_java_test")

host_compatible_java_test(
    name = "test-reasoner",
    srcs = ["ReasonerTest.java"],
    native_libraries_deps = [
        "//:typedb",
        "//database:database",
        "//common:common",
        "//concurrent:concurrent",
        "//logic:logic",
        "//concept:concept",
    ],
    test_class = "com.vaticle.typedb.core.reasoner.ReasonerTest",
    deps = [
        # Internal dependencies
        "//test/integration/util",

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
    ],
)

host_compatible_java_test(
    name = "test-explanation",
    srcs = ["ExplanationTest.java"],
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
    test_class = "com.vaticle.typedb.core.reasoner.ExplanationTest",
    deps = [
        # Internal dependencies
        "//test/integration/util",

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
    ],
)

host_compatible_java_test(
    name = "test-controller",
    srcs = ["controller/ControllerTest.java"],
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
    test_class = "com.vaticle.typedb.core.reasoner.controller.ControllerTest",
    deps = [
        "//test/integration/util",

        # External dependencies from Vaticle
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//common/java:common",
    ],
)

host_compatible_java_test(
    name = "test-answer-count-estimator",
    srcs = ["planner/AnswerCountEstimatorTest.java"],
    native_libraries_deps = [
        # Internal dependencies
        "//database:database",
        "//:typedb",
        "//common:common",
        "//concept:concept",
        "//pattern:pattern",
        "//logic:logic",
        "//reasoner:reasoner",
        "//traversal:traversal",
    ],
    test_class = "com.vaticle.typedb.core.reasoner.planner.AnswerCountEstimatorTest",
    deps = [
        "//test/integration/util",

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java/common",
        "@vaticle_typeql//java:typeql-lang",
    ],
)

host_compatible_java_test(
    name = "test-recursive-planner",
    srcs = ["planner/RecursivePlannerTest.java"],
    native_libraries_deps = [
        # Internal dependencies
        "//database:database",
        "//:typedb",
        "//common:common",
        "//concept:concept",
        "//pattern:pattern",
        "//logic:logic",
        "//reasoner:reasoner",
        "//traversal:traversal",
    ],
    test_class = "com.vaticle.typedb.core.reasoner.planner.RecursivePlannerTest",
    deps = [
        "//test/integration/util",

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
    ],
)

host_compatible_java_test(
    name = "test-reasoner-planner",
    srcs = ["planner/ReasonerPlannerTest.java"],
    native_libraries_deps = [
        # Internal dependencies
        "//database:database",
        "//:typedb",
        "//common:common",
        "//concept:concept",
        "//pattern:pattern",
        "//logic:logic",
        "//reasoner:reasoner",
        "//traversal:traversal",
    ],
    test_class = "com.vaticle.typedb.core.reasoner.planner.ReasonerPlannerTest",
    deps = [
        "//test/integration/util",

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "controller/*",
        "planner/*",
    ]),
    license_type = "mpl-header",
)
