java_test(
    name = "atomic-type-inference-it",
    srcs = ["AtomicTypeInferenceIT.java"],
    test_class = "ai.grakn.graql.internal.reasoner.AtomicTypeInferenceIT",
    deps = [
            "//grakn-graql",
            "//test-integration/rule:cassandra-context",
            "//test-integration/rule:server-context",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//test-integration/util:graql-test-util"
        ],
    resources = ["//test-integration/test/graql/reasoner/resources:type-inference"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "atomic-unification-it",
    srcs = ["AtomicUnificationIT.java"],
    test_class = "ai.grakn.graql.internal.reasoner.AtomicUnificationIT",
    deps = [
            "//grakn-graql",
            "//test-integration/rule:cassandra-context",
            "//test-integration/rule:server-context",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//test-integration/util:graql-test-util"
        ],
    resources = ["//test-integration/test/graql/reasoner/resources:generic-schema"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "atomic-rule-applicability-it",
    srcs = ["AtomicRuleApplicabilityIT.java"],
    test_class = "ai.grakn.graql.internal.reasoner.AtomicRuleApplicabilityIT",
    deps = [
            "//grakn-graql",
            "//test-integration/rule:cassandra-context",
            "//test-integration/rule:server-context",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//test-integration/util:graql-test-util"
        ],
    resources = ["//test-integration/test/graql/reasoner/resources:resource-applicability",
                 "//test-integration/test/graql/reasoner/resources:rule-applicability",
                 "//test-integration/test/graql/reasoner/resources:reified-resource-applicability"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "atomic-role-inference-it",
    srcs = ["AtomicRoleInferenceIT.java"],
    test_class = "ai.grakn.graql.internal.reasoner.AtomicRoleInferenceIT",
    deps = [
            "//grakn-graql",
            "//test-integration/rule:cassandra-context",
            "//test-integration/rule:server-context",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava"
        ],
    resources = ["//test-integration/test/graql/reasoner/resources:role-inference",
                     "//test-integration/test/graql/reasoner/resources:rule-applicability",
                     "//test-integration/test/graql/reasoner/resources:generic-schema"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "atomic-equivalence-it",
    srcs = ["AtomicEquivalenceIT.java"],
    test_class = "ai.grakn.graql.internal.reasoner.AtomicEquivalenceIT",
    deps = [
            "//grakn-graql",
            "//test-integration/rule:cassandra-context",
            "//test-integration/rule:server-context",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava"
        ],
    resources = ["//test-integration/test/graql/reasoner/resources:generic-schema"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)