load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "pattern",
    srcs = glob(["pattern/*.java"]),
    visibility = ["//visibility:public"],
    deps = [
        "@maven//:com_google_guava_guava",
        "//kb/concept/api",
        "@graknlabs_common//:common",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "reasoning-it",
    size = "medium",
    srcs = ["ReasoningIT.java"],
    classpath_resources = ["//test/resources:logback-test"],
    resources = ["//test/integration/graql/reasoner/stubs:reasoning-stubs"],
    test_class = "grakn.core.graql.reasoner.ReasoningIT",
    deps = [
        "//concept/answer",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_collections_commons_collections",
        "@maven//:org_apache_commons_commons_math3",
        "@maven//:org_hamcrest_hamcrest_library",
        "//kb/server",
        "//test/rule:grakn-test-server",
        "//test/common:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "geo-inference-it",
    size = "medium",
    srcs = ["GeoInferenceIT.java"],
    classpath_resources = ["//test/integration/graql/reasoner/resources:reasoner-logback-test"],
    test_class = "grakn.core.graql.reasoner.GeoInferenceIT",
    deps = [
        "//concept/answer",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_collections_commons_collections",
        "@maven//:org_hamcrest_hamcrest_library",
        "//kb/concept/api",
        "//kb/server",
        "//test/integration/graql/reasoner/graph:geo-graph",
        "//test/rule:grakn-test-server",
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
