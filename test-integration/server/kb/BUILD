load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "validator-it",
    size = "medium",
    srcs = ["ValidatorIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.ValidatorIT",
    deps = [
        "//common",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
)

checkstyle_test(
    name = "validator-it-checkstyle",
    config = "//config/checkstyle:checkstyle.xml",
    licenses = ["//config/checkstyle:licenses"],
    suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
    target = ":validator-it",
)

java_test(
    name = "rule-test-it",
    size = "medium",
    srcs = ["RuleTest.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.RuleTest",
    deps = [
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
)

checkstyle_test(
    name = "rule-test-it-checkstyle",
    config = "//config/checkstyle:checkstyle.xml",
    licenses = ["//config/checkstyle:licenses"],
    suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
    target = ":rule-test-it",
)

java_test(
    name = "validate-global-rules-it",
    size = "medium",
    srcs = ["ValidateGlobalRulesIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.ValidateGlobalRulesIT",
    deps = [
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
)

checkstyle_test(
    name = "validate-global-rules-it-checkstyle",
    config = "//config/checkstyle:checkstyle.xml",
    licenses = ["//config/checkstyle:licenses"],
    suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
    target = ":validate-global-rules-it",
)

java_test(
    name = "grakn-tx-it",
    size = "medium",
    srcs = ["TransactionIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.TransactionIT",
    deps = [
        "//common",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server",
        "//test-integration/rule:grakn-test-server",
    ],
)

checkstyle_test(
    name = "grakn-tx-it-checkstyle",
    config = "//config/checkstyle:checkstyle.xml",
    licenses = ["//config/checkstyle:licenses"],
    suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
    target = ":grakn-tx-it",
)
