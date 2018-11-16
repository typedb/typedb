java_test(
    name = "query-equivalence-it",
    srcs = ["AtomicQueryEquivalenceIT.java"],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryEquivalenceIT",
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
java_test(
    name = "atomic-query-it",
    srcs = ["AtomicQueryIT.java"],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryIT",
    deps = [
            "//server",
            "//test-integration/rule:grakn-test-server",
            "//test-integration/graql/reasoner/graph:geo-graph",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//test-integration/util:graql-test-util"
        ],
    resources = ["//test-integration/graql/reasoner/resources:materialisation"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "query-unification-it",
    srcs = ["AtomicQueryUnificationIT.java",
            "pattern/RelationPattern.java",
            "pattern/TypePattern.java",
            "pattern/ResourcePattern.java",
            "pattern/TestQueryPattern.java"],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryUnificationIT",
    deps = [
            "//server",
            "//test-integration/rule:grakn-test-server",
            "//test-integration/graql/reasoner/graph:geo-graph",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//dependencies/maven/artifacts/commons-collections:commons-collections",
        ],
    resources = [
        "//test-integration/graql/reasoner/resources:generic-schema",
        "//test-integration/graql/reasoner/resources:unification-with-types"
        ],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)


java_test(
    name = "query-validity-it",
    size = "medium",
    srcs = ["QueryValidityIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:rule-applicability"],
    test_class = "grakn.core.graql.reasoner.query.QueryValidityIT",
    deps = [
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
)


java_test(
    name = "query-it",
    size = "medium",
    srcs = ["QueryIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.graql.reasoner.query.QueryIT",
    deps = [
        "//server",
        "//test-integration/graql/reasoner/graph:geo-graph",
        "//test-integration/rule:grakn-test-server",
    ],
)

java_test(
    name = "ontological-query-it",
    size = "large",
    srcs = ["OntologicalQueryIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    data = ["//test-integration/graql/reasoner/resources:rule-applicability"],
    test_class = "grakn.core.graql.reasoner.query.OntologicalQueryIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)

java_test(
    name = "resolution-plan-it",
    srcs = ["ResolutionPlanIT.java"],
    test_class = "grakn.core.graql.reasoner.ResolutionPlanIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
    resources = ["//test-integration/graql/reasoner/resources:resolution-plan"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    size = "medium",
)