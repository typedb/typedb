load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "query-equivalence-it",
    size = "medium",
    srcs = ["AtomicQueryEquivalenceIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:generic-schema"],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryEquivalenceIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "query-equivalence-it-checkstyle",
 target = ":query-equivalence-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "atomic-query-it",
    size = "medium",
    srcs = ["AtomicQueryIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:materialisation"],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/graql/reasoner/graph:geo-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "atomic-query-it-checkstyle",
 target = ":atomic-query-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "query-unification-it",
    size = "medium",
    srcs = [
        "AtomicQueryUnificationIT.java",
    ],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = [
        "//test-integration/graql/reasoner/resources:generic-schema",
        "//test-integration/graql/reasoner/resources:unification-with-types",
    ],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryUnificationIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/graql/reasoner/graph:generic-schema-graph",
        "//test-integration/graql/reasoner/graph:geo-graph",
        "//test-integration/graql/reasoner/pattern:query-pattern",
        "//test-integration/graql/reasoner/pattern:relation-pattern",
        "//test-integration/graql/reasoner/pattern:resource-pattern",
        "//test-integration/graql/reasoner/pattern:type-pattern",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "query-unification-it-checkstyle",
 target = ":query-unification-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

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

checkstyle_test(
 name = "query-validity-it-checkstyle",
 target = ":query-validity-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

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

checkstyle_test(
 name = "query-it-checkstyle",
 target = ":query-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

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

checkstyle_test(
 name = "ontological-query-it-checkstyle",
 target = ":ontological-query-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "resolution-plan-it",
    size = "medium",
    srcs = ["ResolutionPlanIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:resolution-plan"],
    test_class = "grakn.core.graql.reasoner.query.ResolutionPlanIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
)

checkstyle_test(
 name = "resolution-plan-it-checkstyle",
 target = ":resolution-plan-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",
 allow_failure = True
)

java_test(
    name = "semantic-difference-it",
    size = "medium",
    srcs = ["SemanticDifferenceIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = [
        "//test-integration/graql/reasoner/resources:generic-schema",
    ],
    test_class = "grakn.core.graql.reasoner.query.SemanticDifferenceIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "semantic-difference-it-checkstyle",
 target = ":semantic-difference-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "subsumption-it",
    size = "medium",
    srcs = ["SubsumptionIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = [
        "//test-integration/graql/reasoner/resources:generic-schema",
    ],
    test_class = "grakn.core.graql.reasoner.query.SubsumptionIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/graql/reasoner/graph:generic-schema-graph",
        "//test-integration/graql/reasoner/pattern:query-pattern",
        "//test-integration/graql/reasoner/pattern:relation-pattern",
        "//test-integration/graql/reasoner/pattern:resource-pattern",
        "//test-integration/graql/reasoner/pattern:type-pattern",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
 name = "subsumption-it-checkstyle",
 target = ":subsumption-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)
