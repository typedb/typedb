load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "validator-it",
    size = "medium",
    srcs = ["ValidatorIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.ValidatorIT",
    deps = [
        "//common",
        "//concept",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "validate-global-rules-it",
    size = "medium",
    srcs = ["ValidateGlobalRulesIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.ValidateGlobalRulesIT",
    deps = [
        "//concept",
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
)

java_test(
    name = "grakn-transaction-it",
    size = "medium",
    srcs = ["TransactionIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.TransactionIT",
    deps = [
        "//common",
        "//concept",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:graql-test-util",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "rule-validation-it",
    size = "large",
    srcs = ["RuleValidationIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.RuleValidationIT",
    deps = [
        "//common",
        "//concept",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":validator-it",
        ":validate-global-rules-it",
        ":grakn-transaction-it",
        ":rule-validation-it",
    ],
)
