load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "validator-it",
    size = "medium",
    srcs = ["ValidatorIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.ValidatorIT",
    deps = [
        "//common:common",
        "//concept:concept",
        "//server:server",

        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
    ],
)

java_test(
    name = "validate-global-rules-it",
    size = "medium",
    srcs = ["ValidateGlobalRulesIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.ValidateGlobalRulesIT",
    deps = [
        "//concept:concept",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
    ],
)

java_test(
    name = "grakn-tx-it",
    size = "medium",
    srcs = ["TransactionIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.TransactionIT",
    deps = [
        "//common:common",
        "//concept:concept",
        "//server:server",
        "//test-integration/rule:grakn-test-server",

        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
    ],
)

java_test(
    name = "rule-it",
    size = "large",
    srcs = ["RuleIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.RuleIT",
    deps = [
        "//common:common",
        "//concept:concept",
        "//server:server",

        "@graknlabs_graql//java:graql",

        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//test-integration/rule:grakn-test-server",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":validator-it",
        ":validate-global-rules-it",
        ":grakn-tx-it",
        ":rule-it",
    ],
)
