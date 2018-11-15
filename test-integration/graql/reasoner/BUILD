java_test(
    name = "reasoning-it",
    srcs = ["ReasoningIT.java"],
    test_class = "grakn.core.graql.reasoner.ReasoningIT",
    deps = [
                "//server",
                "//test-integration/rule:concurrent-grakn-server",
                "//dependencies/maven/artifacts/com/google/guava:guava",
                "//dependencies/maven/artifacts/commons-collections:commons-collections",
                "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
                "//dependencies/maven/artifacts/org/apache/commons:commons-math3"
            ],
    resources = [
            "//test-integration/graql/reasoner/stubs:reflexive-relation",
            "//test-integration/graql/reasoner/stubs:reflexive-symmetric-relation",
            "//test-integration/graql/reasoner/stubs:type-derivation",
            "//test-integration/graql/reasoner/stubs:type-derivation-with-direct",
            "//test-integration/graql/reasoner/stubs:type-derivation-relations-with-direct",
            "//test-integration/graql/reasoner/stubs:type-derivation-from-attribute",
            "//test-integration/graql/reasoner/stubs:type-derivation-from-relations",
            "//test-integration/graql/reasoner/stubs:fresh-entity-derivation-test",
            "//test-integration/graql/reasoner/stubs:fresh-entity-derivation-from-relations",
            "//test-integration/graql/reasoner/stubs:fresh-relation-derivation",
            "//test-integration/graql/reasoner/stubs:appending-rps-test",
            "//test-integration/graql/reasoner/stubs:resource-attachment",
            "//test-integration/graql/reasoner/stubs:resources-as-role-players",
            "//test-integration/graql/reasoner/stubs:test-set7",
            "//test-integration/graql/reasoner/stubs:test-set8",
            "//test-integration/graql/reasoner/stubs:test-set9",
            "//test-integration/graql/reasoner/stubs:test-set10",
            "//test-integration/graql/reasoner/stubs:test-set11",
            "//test-integration/graql/reasoner/stubs:test-set12",
            "//test-integration/graql/reasoner/stubs:test-set13",
            "//test-integration/graql/reasoner/stubs:test-set19",
            "//test-integration/graql/reasoner/stubs:test-set19-recursive",
            "//test-integration/graql/reasoner/stubs:testSet19-recursive.gql",
            "//test-integration/graql/reasoner/stubs:test-set20",
            "//test-integration/graql/reasoner/stubs:test-set21",
            "//test-integration/graql/reasoner/stubs:test-set22",
            "//test-integration/graql/reasoner/stubs:test-set23",
            "//test-integration/graql/reasoner/stubs:test-set24",
            "//test-integration/graql/reasoner/stubs:test-set25",
            "//test-integration/graql/reasoner/stubs:test-set26",
            "//test-integration/graql/reasoner/stubs:test-set27",
            "//test-integration/graql/reasoner/stubs:test-set28",
            "//test-integration/graql/reasoner/stubs:test-set28b",
            "//test-integration/graql/reasoner/stubs:test-set29",
            "//test-integration/graql/reasoner/stubs:test-set30",
            "//test-integration/graql/reasoner/stubs:resource-ownership",
            "//test-integration/graql/reasoner/stubs:resource-hierarchy",
            "//test-integration/graql/reasoner/stubs:resource-directionality"
            ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)
java_test(
    name = "query-validity-it",
    srcs = ["QueryValidityIT.java"],
    test_class = "grakn.core.graql.reasoner.QueryValidityIT",
    deps = [
                "//server",
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
                "//server",
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
                "//server",
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
                "//server",
                "//test-integration/rule:concurrent-grakn-server",
                "//dependencies/maven/artifacts/com/google/guava:guava",
                "//test-integration/util:graql-test-util"
            ],
    size = "large",
    data = ["//test-integration/graql/reasoner/resources:rule-applicability"],
    classpath_resources = ["//test-integration/resources:logback-test"]
)
