load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "type-inference-it",
    size = "medium",
    srcs = ["TypeInferenceIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.graql.reasoner.atomic.TypeInferenceIT",
    deps = [
        "//common",
        "//concept/answer",
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test/common:graql-test-util",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "atomic-unification-it",
    size = "medium",
    srcs = ["AtomicUnificationIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/resources:generic-schema"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicUnificationIT",
    deps = [
        "//common",
        "//concept/answer",
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test/common:graql-test-util",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "atomic-conversion-it",
    size = "medium",
    srcs = ["AtomicConversionIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/resources:generic-schema"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicConversionIT",
    deps = [
        "//common",
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
        "//graql/reasoner",
        "//kb/server",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "rule-applicability-it",
    size = "medium",
    srcs = ["RuleApplicabilityIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = [
        "//test/integration/graql/reasoner/resources:explicit-ownership-relation-applicability",
        "//test/integration/graql/reasoner/resources:resource-applicability",
        "//test/integration/graql/reasoner/resources:rule-applicability-new",
    ],
    test_class = "grakn.core.graql.reasoner.atomic.RuleApplicabilityIT",
    deps = [
        "//common",
        "//core",
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/server",
        "//test/common:graql-test-util",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "role-inference-it",
    size = "medium",
    srcs = ["RoleInferenceIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.graql.reasoner.atomic.RoleInferenceIT",
    deps = [
        "//common",
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/server",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql"
    ],
)

java_test(
    name = "atomic-equivalence-it",
    size = "medium",
    srcs = ["AtomicEquivalenceIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/resources:generic-schema"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicEquivalenceIT",
    deps = [
        "//common",
        "//core",
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":atomic-conversion-it",
        ":atomic-equivalence-it",
        ":atomic-unification-it",
        ":type-inference-it",
        ":rule-applicability-it",
        ":role-inference-it",
    ],
)
