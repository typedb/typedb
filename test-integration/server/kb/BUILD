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

# TODO: remove
java_test(
    name = "gsk-it",
    size = "medium",
    srcs = ["GskIT.java"],
    test_class = "grakn.core.server.kb.GskIT",
    deps = [
            # Internal dependencies
            "//common:common",
            "//concept:concept",
            "//server:server",

            # External dependencies from @graknlabs
            "@graknlabs_client_java//:client-java",
            "@graknlabs_graql//java:graql",

            # External dependencies from Maven
            "//dependencies/maven/artifacts/commons-io:commons-io",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
            "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
        ],
    data = ["//:assemble-mac-zip"],
    classpath_resources = ["//test-integration/resources:logback-test"]
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
        ":rule-validation-it",
        ":gsk-it" # TODO: remove
    ],
)
