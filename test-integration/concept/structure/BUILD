load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "casting-it",
    size = "medium",
    srcs = ["CastingIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.structure.CastingIT",
    deps = [
        "//concept/impl",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//kb/concept/structure",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:concept-downcasting",
    ],
)

java_test(
    name = "edge-it",
    size = "medium",
    srcs = ["EdgeIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.structure.EdgeIT",
    deps = [
        "//concept/structure",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//kb/server",
        "//kb/concept/api",
        "//kb/concept/structure",
        "//server",
        "//common",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:concept-downcasting",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":casting-it",
        ":edge-it",
    ],
)
