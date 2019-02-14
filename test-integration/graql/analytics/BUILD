load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "graql-compute-it",
    test_class = "grakn.core.graql.analytics.GraqlComputeIT",
    srcs = ["GraqlComputeIT.java"],
    deps = [
        "//graql/java:graql",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "connected-component-it",
    test_class = "grakn.core.graql.analytics.ConnectedComponentIT",
    srcs = ["ConnectedComponentIT.java"],
    deps = [
        "//graql/java:graql",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "coreness-it",
    test_class = "grakn.core.graql.analytics.CorenessIT",
    srcs = ["CorenessIT.java"],
    deps = [
        "//graql/java:graql",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "count-it",
    test_class = "grakn.core.graql.analytics.CountIT",
    srcs = ["CountIT.java"],
    deps = [
        "//graql/java:graql",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "degree-it",
    test_class = "grakn.core.graql.analytics.DegreeIT",
    srcs = ["DegreeIT.java"],
    deps = [
        "//graql/java:graql",
                "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "kcore-it",
    test_class = "grakn.core.graql.analytics.KCoreIT",
    srcs = ["KCoreIT.java"],
    deps = [
        "//graql/java:graql",
                "//server:server",
        "//test-integration/rule:grakn-test-server",
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "path-it",
    test_class = "grakn.core.graql.analytics.PathIT",
    srcs = ["PathIT.java"],
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
    name = "statistics-it",
    test_class = "grakn.core.graql.analytics.StatisticsIT",
    srcs = ["StatisticsIT.java"],
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
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
        ":statistics-it"
    ]
)
