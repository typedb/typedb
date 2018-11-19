java_test(
    name = "reasoning-it",
    size = "enormous",
    srcs = ["ReasoningIT.java"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.ReasoningIT",
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)
