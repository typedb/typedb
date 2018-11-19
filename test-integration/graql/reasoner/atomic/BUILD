java_test(
    name = "atomic-type-inference-it",
    srcs = ["AtomicTypeInferenceIT.java"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicTypeInferenceIT",
    deps = [
            "//server",
            "//test-integration/rule:grakn-test-server",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//test-integration/util:graql-test-util"
        ],
    resources = ["//test-integration/graql/reasoner/resources:type-inference"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "atomic-unification-it",
    srcs = ["AtomicUnificationIT.java"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicUnificationIT",
    deps = [
            "//server",
                        "//test-integration/rule:grakn-test-server",

            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//test-integration/util:graql-test-util"
        ],
    resources = ["//test-integration/graql/reasoner/resources:generic-schema"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "atomic-rule-applicability-it",
    srcs = ["AtomicRuleApplicabilityIT.java"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicRuleApplicabilityIT",
    deps = [
            "//server",
            "//test-integration/rule:grakn-test-server",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//test-integration/util:graql-test-util"
        ],
    resources = ["//test-integration/graql/reasoner/resources:resource-applicability",
                 "//test-integration/graql/reasoner/resources:rule-applicability",
                 "//test-integration/graql/reasoner/resources:reified-resource-applicability"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "atomic-role-inference-it",
    srcs = ["AtomicRoleInferenceIT.java"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicRoleInferenceIT",
    deps = [
            "//server",
            "//test-integration/rule:grakn-test-server",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava"
        ],
    resources = ["//test-integration/graql/reasoner/resources:role-inference",
                     "//test-integration/graql/reasoner/resources:rule-applicability",
                     "//test-integration/graql/reasoner/resources:generic-schema"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "atomic-equivalence-it",
    srcs = ["AtomicEquivalenceIT.java"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicEquivalenceIT",
    deps = [
            "//server",
            "//test-integration/rule:grakn-test-server",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava"
        ],
    resources = ["//test-integration/graql/reasoner/resources:generic-schema"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)