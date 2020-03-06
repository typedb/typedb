load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "graql-compute-it",
    size = "large",
    srcs = ["GraqlComputeIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.graql.analytics.GraqlComputeIT",
    deps = [
        "//concept/answer",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//kb/graql/exception",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "connected-component-it",
    size = "large",
    srcs = ["ConnectedComponentIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.graql.analytics.ConnectedComponentIT",
    deps = [
        "//concept/answer",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//kb/graql/exception",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "coreness-it",
    size = "large",
    srcs = ["CorenessIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.graql.analytics.CorenessIT",
    deps = [
        "//concept/answer",
        "//kb/concept/api",
        "//kb/graql/exception",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "count-it",
    size = "large",
    srcs = ["CountIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.graql.analytics.CountIT",
    deps = [
        "//concept/answer",
        "//core",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "degree-it",
    size = "large",
    srcs = ["DegreeIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.graql.analytics.DegreeIT",
    deps = [
        "//concept/answer",
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "kcore-it",
    size = "large",
    srcs = ["KCoreIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.graql.analytics.KCoreIT",
    deps = [
        "//concept/answer",
        "//kb/concept/api",
        "//kb/graql/exception",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "path-it",
    size = "large",
    srcs = ["PathIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.graql.analytics.PathIT",
    deps = [
        "//common",
        "//concept/answer",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//graql/analytics",
        "//graql/executor",
        "//graql/reasoner",
        "//kb/concept/api",
        "//kb/concept/manager",
        "//kb/graql/exception",
        "//kb/graql/planning",
        "//kb/graql/executor",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "statistics-it",
    size = "large",
    srcs = ["StatisticsIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.graql.analytics.StatisticsIT",
    deps = [
        "//concept/answer",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//kb/concept/api",
        "//kb/graql/exception",
        "//kb/server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":graql-compute-it",
        ":connected-component-it",
        ":coreness-it",
        ":count-it",
        ":degree-it",
        ":kcore-it",
        ":path-it",
        ":statistics-it",
    ],
)
