load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "casting-it",
    size = "medium",
    srcs = ["CastingIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.concept.structure.CastingIT",
    deps = [
        "//concept/impl",
        "@maven//:org_hamcrest_hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//test/rule:grakn-test-server",
        "//test/integration/util:concept-downcasting",
    ],
)

java_test(
    name = "edge-it",
    size = "medium",
    srcs = ["EdgeIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    test_class = "grakn.core.concept.structure.EdgeIT",
    deps = [
        "//concept/structure",
        "//core",
        "@maven//:com_google_guava_guava",
        "@maven//:org_apache_tinkerpop_gremlin-core",
        "@maven//:org_mockito_mockito-core",
        "//kb/server",
        "//kb/concept/api",
        "//kb/concept/structure",
        "//server",
        "//common",
        "//test/rule:grakn-test-server",
        "//test/integration/util:concept-downcasting",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":casting-it",
        ":edge-it",
    ],
)
