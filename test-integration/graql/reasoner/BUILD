load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "reasoning-it",
    size = "enormous",
    srcs = ["ReasoningIT.java"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.ReasoningIT",
    deps = [
        "@graknlabs_graql//java:graql",
        "//server:server",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)

java_test(
    name = "geo-inference-it",
    size = "medium",
    srcs = ["GeoInferenceIT.java"],
    classpath_resources = [
        "//test-integration/resources:logback-test",
        "//test-integration/resources:cassandra-embedded",
    ],
    test_class = "grakn.core.graql.reasoner.GeoInferenceIT",
    deps = [
        "@graknlabs_graql//java:graql",
        "//server:server",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//test-integration/graql/reasoner/graph:geo-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":reasoning-it",
        ":geo-inference-it",
    ]
)
