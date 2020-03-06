load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "pattern",
    srcs = glob(["pattern/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "reasoning-it",
    size = "large",
    srcs = ["ReasoningIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    resources = ["//test-integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.ReasoningIT",
    deps = [
        "//concept/answer",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/apache/commons:commons-math3",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "geo-inference-it",
    size = "medium",
    srcs = ["GeoInferenceIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.graql.reasoner.GeoInferenceIT",
    deps = [
        "//concept/answer",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-collections",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/graql/reasoner/graph:geo-graph",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":reasoning-it",
        ":geo-inference-it",
        ":pattern",
    ],
)
