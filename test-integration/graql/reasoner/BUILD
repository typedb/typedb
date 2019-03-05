load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "reasoning-it",
    size = "enormous",
    srcs = ["ReasoningIT.java"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.ReasoningIT",
    deps = [
        "//concept:concept",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",

        "@graknlabs_graql//java:graql",

        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
    ],
    classpath_resources = ["//test-integration/resources:logback-test"],
)

java_test(
    name = "geo-inference-it",
    size = "medium",
    srcs = ["GeoInferenceIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.graql.reasoner.GeoInferenceIT",
    deps = [
        "//concept:concept",
        "//server:server",
        "//test-integration/graql/reasoner/graph:geo-graph",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",

        "@graknlabs_graql//java:graql",

        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":reasoning-it",
        ":geo-inference-it",
    ]
)
