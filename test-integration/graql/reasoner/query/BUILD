load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "query-test-util",
    srcs = glob(["QueryTestUtil.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/junit",
        "//graql/reasoner",
        "//kb/graql/reasoner",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "query-equivalence-it",
    size = "medium",
    srcs = [
        "AtomicQueryEquivalenceIT.java",
    ],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:generic-schema"],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryEquivalenceIT",
    deps = [
        "//common",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//graql/reasoner",
        "//kb/server",
        "//test-integration/graql/reasoner/query:query-test-util",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "atomic-query-it",
    size = "medium",
    srcs = ["AtomicQueryIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:generic-schema"],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryIT",
    deps = [
        "//common",
        "//concept/answer",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//graql/reasoner",
        "//kb/graql/exception",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "explanation-it",
    size = "medium",
    srcs = [
        "ExplanationIT.java",
    ],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = [
        "//test-integration/graql/reasoner/stubs:reasoning-stubs",
    ],
    test_class = "grakn.core.graql.reasoner.query.ExplanationIT",
    deps = [
        "//concept/answer",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/graql/reasoner/graph:geo-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "materialisation-it",
    size = "medium",
    srcs = ["MaterialisationIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:materialisation"],
    test_class = "grakn.core.graql.reasoner.query.MaterialisationIT",
    deps = [
        "//common",
        "//concept/answer",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "negation-it",
    size = "medium",
    srcs = [
        "NegationIT.java",
    ],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = [
        "//test-integration/graql/reasoner/resources/recursion:recursion-schemas",
        "//test-integration/graql/reasoner/stubs:reasoning-stubs",
    ],
    test_class = "grakn.core.graql.reasoner.query.NegationIT",
    deps = [
        "//common",
        "//concept/answer",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/graql/exception",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test-integration/graql/reasoner/graph:reachability-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
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
        "//common",
        "//concept/answer",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test-integration/graql/reasoner:pattern",
        "//test-integration/graql/reasoner/graph:generic-schema-graph",
        "//test-integration/graql/reasoner/query:query-test-util",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "query-it",
    size = "medium",
    srcs = ["QueryIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.graql.reasoner.query.QueryIT",
    deps = [
        "//common",
        "//concept/answer",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/graql/reasoner/graph:geo-graph",
        "//test-integration/rule:grakn-test-server",
        "@com-google-guava-guava//jar",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "ontological-query-it",
    size = "large",
    srcs = ["OntologicalQueryIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = [
        "//test-integration/graql/reasoner/resources:matching-types",
        "//test-integration/graql/reasoner/resources:rule-applicability",
    ],
    test_class = "grakn.core.graql.reasoner.query.OntologicalQueryIT",
    deps = [
        "//concept/answer",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "resolution-plan-it",
    size = "medium",
    srcs = ["ResolutionPlanIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/resources:resolution-plan"],
    test_class = "grakn.core.graql.reasoner.query.ResolutionPlanIT",
    deps = [
        "//common",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//graql/planning",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/graql/planning",
        "//kb/graql/reasoner",
        "//kb/keyspace",
        "//kb/server",
        "//keyspace",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
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
        "//common",
        "//concept/answer",
        "//dependencies/maven/artifacts/com/google/guava",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "generative-operational-it",
    size = "enormous",
    srcs = [
        "GenerativeOperationalIT.java",
        "TransactionContext.java",
    ],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = [
        "//test-integration/graql/reasoner/resources:generic-schema",
    ],
    test_class = "grakn.core.graql.reasoner.query.GenerativeOperationalIT",
    deps = [
        "//common",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/graql/reasoner/query:query-test-util",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
        "@graknlabs_verification//tools:operator",
    ],
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
        "//common",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//graql/reasoner",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test-integration/graql/reasoner:pattern",
        "//test-integration/graql/reasoner/graph:generic-schema-graph",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":query-test-util",
        ":atomic-query-it",
        ":query-equivalence-it",
        ":explanation-it",
        ":materialisation-it",
        ":negation-it",
        ":query-unification-it",
        ":query-it",
        ":ontological-query-it",
        ":resolution-plan-it",
        ":semantic-difference-it",
        ":subsumption-it",
        ":generative-operational-it",
    ],
)
