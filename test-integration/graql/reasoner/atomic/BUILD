load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "type-inference-it",
    size = "medium",
    srcs = ["TypeInferenceIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:type-inference"],
    test_class = "grakn.core.graql.reasoner.atomic.TypeInferenceIT",
    deps = [
        "//concept",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
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
    test_class = "grakn.core.graql.reasoner.atomic.AtomicUnificationIT",
    deps = [
        "//concept",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
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
    test_class = "grakn.core.graql.reasoner.atomic.RuleApplicabilityIT",
    deps = [
        "//concept",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
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
    test_class = "grakn.core.graql.reasoner.atomic.RoleInferenceIT",
    deps = [
        "//concept",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "atomic-equivalence-it",
    size = "medium",
    srcs = ["AtomicEquivalenceIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:generic-schema"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicEquivalenceIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":atomic-equivalence-it",
        ":atomic-unification-it",
        ":type-inference-it",
        ":rule-applicability-it",
        ":role-inference-it",
    ],
)
