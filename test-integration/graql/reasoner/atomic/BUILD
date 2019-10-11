load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "type-inference-it",
    size = "medium",
    srcs = ["TypeInferenceIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:type-inference"],
    test_class = "grakn.core.kb.graql.reasoner.atomic.TypeInferenceIT",
    deps = [
        "//kb:kb",
        "//graql:graql",
        "//kb/concept/api:api",
        "//concept/answer:answer",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "atomic-unification-it",
    size = "medium",
    srcs = ["AtomicUnificationIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:generic-schema"],
    test_class = "grakn.core.kb.graql.reasoner.atomic.AtomicUnificationIT",
    deps = [
        "//kb:kb",
        "//graql:graql",
        "//core:core",
        "//kb/concept/api:api",
        "//concept/answer:answer",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "atomic-conversion-it",
    size = "medium",
    srcs = ["AtomicConversionIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:generic-schema"],
    test_class = "grakn.core.kb.graql.reasoner.atomic.AtomicConversionIT",
    deps = [
        "//kb:kb",
        "//graql:graql",
        "//core:core",
        "//kb/concept/api:api",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "rule-applicability-it",
    size = "medium",
    srcs = ["RuleApplicabilityIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = [
        "//test-integration/graql/reasoner/resources:reified-resource-applicability",
        "//test-integration/graql/reasoner/resources:resource-applicability",
        "//test-integration/graql/reasoner/resources:rule-applicability",
    ],
    test_class = "grakn.core.kb.graql.reasoner.atomic.RuleApplicabilityIT",
    deps = [
        "//kb:kb",
        "//graql:graql",
        "//core:core",
        "//kb/concept/api:api",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "role-inference-it",
    size = "medium",
    srcs = ["RoleInferenceIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = [
        "//test-integration/graql/reasoner/resources:generic-schema",
        "//test-integration/graql/reasoner/resources:role-inference",
        "//test-integration/graql/reasoner/resources:rule-applicability",
    ],
    test_class = "grakn.core.kb.graql.reasoner.atomic.RoleInferenceIT",
    deps = [
        "//kb:kb",
        "//graql:graql",
        "//kb/concept/api:api",
        "//core:core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "atomic-equivalence-it",
    size = "medium",
    srcs = ["AtomicEquivalenceIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:generic-schema"],
    test_class = "grakn.core.kb.graql.reasoner.atomic.AtomicEquivalenceIT",
    deps = [
        "//kb/concept/api:api",
        "//server:server",
        "//graql:graql",
        "//kb:kb",
        "//kb/concept/util:util",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
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
