load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "neq-idpredicate-it",
    size = "medium",
    srcs = ["NeqIdPredicateIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.kb.reasoner.reasoning.NeqIdPredicateIT",
    deps = [
        "//kb/concept/api:api",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "valuepredicate-it",
    size = "medium",
    srcs = ["ValuePredicateIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.kb.reasoner.reasoning.ValuePredicateIT",
    deps = [
        "//kb/concept/api:api",
        "//dependencies/maven/artifacts/com/google/guava",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "recursion-it",
    size = "large",
    srcs = ["RecursionIT.java"],
    classpath_resources = [
        "//test-integration/resources:logback-test",
        "//test-integration/resources:cassandra-embedded",
    ],
    resources = [
        "//test-integration/graql/reasoner/resources/recursion:nguyen",
        "//test-integration/graql/reasoner/resources/recursion:recursion-schemas",
    ],
    test_class = "grakn.core.kb.reasoner.reasoning.RecursionIT",
    deps = [
        "//kb/concept/api:api",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server:server",
        "//test-integration/graql/reasoner/graph:dual-linear-transitivity-graph",
        "//test-integration/graql/reasoner/graph:linear-transitivity-matrix-graph",
        "//test-integration/graql/reasoner/graph:nguyen-graph",
        "//test-integration/graql/reasoner/graph:path-matrix-graph",
        "//test-integration/graql/reasoner/graph:path-tree-graph",
        "//test-integration/graql/reasoner/graph:path-tree-symmetric-graph",
        "//test-integration/graql/reasoner/graph:reachability-graph",
        "//test-integration/graql/reasoner/graph:tail-recursion-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "attribute-attachment-it",
    size = "medium",
    srcs = ["AttributeAttachmentIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.kb.reasoner.reasoning.AttributeAttachmentIT",
    deps = [
        "//kb/concept/api:api",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "type-generation-it",
    size = "medium",
    srcs = ["TypeGenerationIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.kb.reasoner.reasoning.TypeGenerationIT",
    deps = [
        "//kb/concept/api:api",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "type-hierarchies-it",
    size = "medium",
    srcs = ["TypeHierarchiesIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.kb.reasoner.reasoning.TypeHierarchiesIT",
    deps = [
        "//kb:kb",
        "//kb/concept/api:api",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "variable-roles-it",
    size = "medium",
    srcs = ["VariableRolesIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.kb.reasoner.reasoning.VariableRolesIT",
    deps = [
        "//kb/concept/api:api",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":attribute-attachment-it",
        ":neq-idpredicate-it",
        ":valuepredicate-it",
        ":recursion-it",
        ":type-generation-it",
        ":type-hierarchies-it",
        ":variable-roles-it",
    ],
)
