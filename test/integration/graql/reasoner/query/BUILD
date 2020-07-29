load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "query-test-util",
    srcs = glob(["QueryTestUtil.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//graql/reasoner",
        "//kb/graql/reasoner",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
        "@maven//:junit_junit",
    ],
)

java_test(
    name = "query-equivalence-it",
    size = "medium",
    srcs = [
        "AtomicQueryEquivalenceIT.java",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/resources:generic-schema"],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryEquivalenceIT",
    deps = [
        "//common",
        "//graql/reasoner",
        "//kb/server",
        "//test/integration/graql/reasoner/query:query-test-util",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
    ],
)

java_test(
    name = "atomic-query-it",
    size = "medium",
    srcs = ["AtomicQueryIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/resources:generic-schema"],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryIT",
    deps = [
        "//common",
        "//concept/answer",
        "//graql/reasoner",
        "//kb/graql/exception",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test/common:graql-test-util",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
    ],
)

java_test(
    name = "explanation-it",
    size = "medium",
    srcs = [
        "ExplanationIT.java",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    resources = [
        "//test/integration/graql/reasoner/stubs:reasoning-stubs",
    ],
    test_class = "grakn.core.graql.reasoner.query.ExplanationIT",
    deps = [
        "//common",
        "//concept/answer",
        "//kb/concept/api",
        "//kb/server",
        "//test/common:graql-test-util",
        "//test/integration/graql/reasoner/graph:geo-graph",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_collections_commons_collections",
        "@maven//:org_hamcrest_hamcrest_library",
    ],
)

java_test(
    name = "materialisation-it",
    size = "medium",
    srcs = ["MaterialisationIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/resources:materialisation"],
    test_class = "grakn.core.graql.reasoner.query.MaterialisationIT",
    deps = [
        "//common",
        "//concept/answer",
        "//concept/impl",
        "//core",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/concept/structure",
        "//kb/server",
        "//test/common:graql-test-util",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_tinkerpop_gremlin_core",
        "@maven//:org_hamcrest_hamcrest_library",
    ],
)

java_test(
    name = "negation-it",
    size = "medium",
    srcs = [
        "NegationIT.java",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    resources = [
        "//test/integration/graql/reasoner/resources/recursion:recursion-schemas",
        "//test/integration/graql/reasoner/stubs:reasoning-stubs",
    ],
    test_class = "grakn.core.graql.reasoner.query.NegationIT",
    deps = [
        "//common",
        "//concept/answer",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/graql/exception",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test/common:graql-test-util",
        "//test/integration/graql/reasoner/graph:reachability-graph",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_collections_commons_collections",
        "@maven//:org_hamcrest_hamcrest_library",
    ],
)

java_test(
    name = "query-unification-it",
    size = "medium",
    srcs = [
        "AtomicQueryUnificationIT.java",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    resources = [
        "//test/integration/graql/reasoner/resources:generic-schema-refactored",
        "//test/integration/graql/reasoner/resources:unification-with-types",
    ],
    test_class = "grakn.core.graql.reasoner.query.AtomicQueryUnificationIT",
    deps = [
        "//common",
        "//concept/answer",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test/common:graql-test-util",
        "//test/integration/graql/reasoner:pattern",
        "//test/integration/graql/reasoner/graph:generic-schema-graph",
        "//test/integration/graql/reasoner/query:query-test-util",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_collections_commons_collections",
        "@maven//:org_hamcrest_hamcrest_library",
    ],
)

java_test(
    name = "query-it",
    size = "medium",
    srcs = ["QueryIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.graql.reasoner.query.QueryIT",
    deps = [
        "//common",
        "//concept/answer",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/server",
        "//test/integration/graql/reasoner/graph:geo-graph",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
    ],
)

java_test(
    name = "resolution-plan-it",
    size = "medium",
    srcs = ["ResolutionPlanIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/resources:resolution-plan"],
    test_class = "grakn.core.graql.reasoner.query.ResolutionPlanIT",
    deps = [
        "//common",
        "//graql/planning",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/graql/planning",
        "//kb/graql/reasoner",
        "//kb/keyspace",
        "//kb/server",
        "//keyspace",
        "//test/common:graql-test-util",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
    ],
)

java_test(
    name = "generative-operational-it",
    size = "enormous",
    srcs = [
        "GenerativeOperationalIT.java",
        "TransactionContext.java",
    ],
    classpath_resources = ["//test/resources:logback-test"],
    resources = [
        "//test/integration/graql/reasoner/resources:generic-schema-refactored",
    ],
    test_class = "grakn.core.graql.reasoner.query.GenerativeOperationalIT",
    deps = [
        "//common",
        "//core",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/server",
        "//test/common:graql-test-util",
        "//test/integration/graql/reasoner/query:query-test-util",
        "//test/rule:grakn-test-server",
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
        "@graknlabs_verification//tools/operator",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_collections_commons_collections",
        "@maven//:org_hamcrest_hamcrest_library",
    ],
)

java_test(
    name = "subsumption-it",
    size = "medium",
    srcs = ["SubsumptionIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = [
        "//test/integration/graql/reasoner/resources:generic-schema-refactored",
    ],
    test_class = "grakn.core.graql.reasoner.query.SubsumptionIT",
    deps = [
        "//common",
        "//graql/reasoner",
        "//kb/graql/reasoner",
        "//kb/server",
        "//test/integration/graql/reasoner:pattern",
        "//test/integration/graql/reasoner/graph:generic-schema-graph",
        "//test/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_collections_commons_collections",
        "@maven//:org_hamcrest_hamcrest_library",
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
        ":resolution-plan-it",
        ":subsumption-it",
        ":generative-operational-it",
    ],
)
