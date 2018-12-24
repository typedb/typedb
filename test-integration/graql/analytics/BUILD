load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "analytics-it",
    test_class = "grakn.core.graql.analytics.ComputeQueryIT",
    srcs = ["ComputeQueryIT.java"],
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
 name = "analytics-it-checkstyle",
 target = ":analytics-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_test(
    name = "connected-component-it",
    test_class = "grakn.core.graql.analytics.ConnectedComponentIT",
    srcs = ["ConnectedComponentIT.java"],
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
 name = "connected-component-it-checkstyle",
 target = ":connected-component-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_test(
    name = "coreness-it",
    test_class = "grakn.core.graql.analytics.CorenessIT",
    srcs = ["CorenessIT.java"],
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
 name = "coreness-it-checkstyle",
 target = ":coreness-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_test(
    name = "count-it",
    test_class = "grakn.core.graql.analytics.CountIT",
    srcs = ["CountIT.java"],
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
 name = "count-it-checkstyle",
 target = ":count-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_test(
    name = "degree-it",
    test_class = "grakn.core.graql.analytics.DegreeIT",
    srcs = ["DegreeIT.java"],
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/com/google/guava:guava"
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
 name = "degree-it-checkstyle",
 target = ":degree-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_test(
    name = "kcore-it",
    test_class = "grakn.core.graql.analytics.KCoreIT",
    srcs = ["KCoreIT.java"],
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
 name = "kcore-it-checkstyle",
 target = ":kcore-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

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

checkstyle_test(
 name = "path-it-checkstyle",
 target = ":path-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

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
 name = "statistics-it-checkstyle",
 target = ":statistics-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)
