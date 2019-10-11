load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "validator-it",
    size = "medium",
    srcs = ["ValidatorIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.server.kb.ValidatorIT",
    deps = [
        "//common",
        "//kb:kb",
        "//kb/concept/structure:structure",
        "//kb/concept/api:api",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server:server",
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
        "//kb/concept/api:api",
        "//kb:kb",
        "//kb/concept/structure:structure",
        "//concept/impl:concept",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:concept-downcasting",
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
        "//kb/concept/api:api",
        "//kb:kb",
        "//graql:graql",
        "//core:core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//server:server",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":validator-it",
        ":validate-global-rules-it",
        ":rule-validation-it",
    ],
)
