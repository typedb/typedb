java_test(
    name = "isa-explicit-it",
    test_class = "grakn.core.graql.internal.IsaExplicitIT",
    srcs = ["IsaExplicitIT.java"],
    deps = [
        "//test-integration/rule:grakn-test-server",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//server"],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)