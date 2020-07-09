load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "neq-idpredicate-it",
    size = "medium",
    srcs = ["NeqIdPredicateIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.reasoning.NeqIdPredicateIT",
    deps = [
        "//concept/answer",
        "//kb/server",
        "//test/rule:grakn-test-server",
        "//test/common:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "valuepredicate-it",
    size = "medium",
    srcs = ["ValuePredicateIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.reasoning.ValuePredicateIT",
    deps = [
        "//concept/answer",
        "@maven//:com_google_guava_guava",
        "//graql/reasoner",
        "//kb/server",
        "//kb/concept/api",
        "//common",
        "//test/rule:grakn-test-server",
        "//test/common:graql-test-util",
        "@graknlabs_graql//java:graql",
        "@graknlabs_common//:common",
    ],
)

java_test(
    name = "recursion-it",
    size = "large",
    srcs = ["RecursionIT.java"],
    classpath_resources = [
        "//test/resources:logback-test",
        "//test/resources:cassandra-embedded",
    ],
    resources = [
        "//test/integration/graql/reasoner/resources/recursion:nguyen",
        "//test/integration/graql/reasoner/resources/recursion:recursion-schemas",
    ],
    test_class = "grakn.core.graql.reasoner.reasoning.RecursionIT",
    deps = [
        "//concept/answer",
        "@maven//:com_google_guava_guava",
        "@maven//:org_hamcrest_hamcrest_library",
        "//kb/server",
        "//test/integration/graql/reasoner/graph:dual-linear-transitivity-graph",
        "//test/integration/graql/reasoner/graph:linear-transitivity-matrix-graph",
        "//test/integration/graql/reasoner/graph:nguyen-graph",
        "//test/integration/graql/reasoner/graph:path-matrix-graph",
        "//test/integration/graql/reasoner/graph:path-tree-graph",
        "//test/integration/graql/reasoner/graph:path-tree-symmetric-graph",
        "//test/integration/graql/reasoner/graph:reachability-graph",
        "//test/integration/graql/reasoner/graph:tail-recursion-graph",
        "//test/rule:grakn-test-server",
        "//test/common:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "type-generation-it",
    size = "medium",
    srcs = ["TypeGenerationIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.reasoning.TypeGenerationIT",
    deps = [
        "//concept/answer",
        "//kb/server",
        "//test/rule:grakn-test-server",
        "//test/common:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "type-hierarchies-it",
    size = "medium",
    srcs = ["TypeHierarchiesIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.reasoning.TypeHierarchiesIT",
    deps = [
        "//concept/answer",
        "@maven//:org_hamcrest_hamcrest_library",
        "//kb/server",
        "//test/rule:grakn-test-server",
        "//test/common:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "variable-roles-it",
    size = "large",
    srcs = ["VariableRolesIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.reasoning.VariableRolesIT",
    deps = [
        "//concept/answer",
        "@maven//:commons_collections_commons_collections",
        "@maven//:org_apache_commons_commons_math3",
        "//kb/server",
        "//test/rule:grakn-test-server",
        "//test/common:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":neq-idpredicate-it",
        ":valuepredicate-it",
        ":recursion-it",
        ":type-generation-it",
        ":type-hierarchies-it",
        ":variable-roles-it",
    ],
)
