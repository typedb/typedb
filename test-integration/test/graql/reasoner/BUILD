java_test(
    name = "query-validity-it",
    srcs = ["QueryValidityIT.java"],
    test_class = "ai.grakn.graql.internal.reasoner.QueryValidityIT",
    deps = [
                "//grakn-graql",
                "//test-integration/rule:grakn-server",
                "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
            ],
    resources = ["//test-integration/test/graql/reasoner/resources:rule-applicability"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "resolution-plan-it",
    srcs = ["ResolutionPlanIT.java"],
    test_class = "ai.grakn.graql.internal.reasoner.ResolutionPlanIT",
    deps = [
                "//grakn-graql",
                "//test-integration/rule:grakn-server",
                "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
                "//dependencies/maven/artifacts/com/google/guava:guava"
            ],
    resources = ["//test-integration/test/graql/reasoner/resources:resolution-plan"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "query-it",
    srcs = ["QueryIT.java"],
    test_class = "ai.grakn.graql.internal.reasoner.QueryIT",
    deps = [
                "//grakn-graql",
                "//test-integration/rule:grakn-server",
                "//test-integration/test/graql/reasoner/graph:geo-graph"
            ],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "ontological-query-it",
    srcs = ["OntologicalQueryIT.java"],
    test_class = "ai.grakn.graql.internal.reasoner.OntologicalQueryIT",
    deps = [
                "//grakn-graql",
                "//test-integration/rule:grakn-server",
                "//dependencies/maven/artifacts/com/google/guava:guava",
                "//test-integration/util:graql-test-util"
            ],
    size = "medium",
    resources = ["//test-integration/test/graql/reasoner/resources:rule-applicability"],
    classpath_resources = ["//test-integration/resources:logback-test"]
)

