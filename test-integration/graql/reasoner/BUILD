java_test(
    name = "query-validity-it",
    srcs = ["QueryValidityIT.java"],
    test_class = "grakn.core.graql.reasoner.QueryValidityIT",
    deps = [
                "//grakn-graql",
                "//test-integration/rule:concurrent-grakn-server",
                "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
            ],
    resources = ["//test-integration/graql/reasoner/resources:rule-applicability"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "resolution-plan-it",
    srcs = ["ResolutionPlanIT.java"],
    test_class = "grakn.core.graql.reasoner.ResolutionPlanIT",
    deps = [
                "//grakn-graql",
                "//test-integration/rule:concurrent-grakn-server",
                "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
                "//dependencies/maven/artifacts/com/google/guava:guava"
            ],
    resources = ["//test-integration/graql/reasoner/resources:resolution-plan"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "query-it",
    srcs = ["QueryIT.java"],
    test_class = "grakn.core.graql.reasoner.QueryIT",
    deps = [
                "//grakn-graql",
                "//test-integration/rule:concurrent-grakn-server",
                "//test-integration/graql/reasoner/graph:geo-graph"
            ],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "ontological-query-it",
    srcs = ["OntologicalQueryIT.java"],
    test_class = "grakn.core.graql.reasoner.OntologicalQueryIT",
    deps = [
                "//grakn-graql",
                "//test-integration/rule:concurrent-grakn-server",
                "//dependencies/maven/artifacts/com/google/guava:guava",
                "//test-integration/util:graql-test-util"
            ],
    size = "large",
    data = ["//test-integration/graql/reasoner/resources:rule-applicability"],
    classpath_resources = ["//test-integration/resources:logback-test"]
)
