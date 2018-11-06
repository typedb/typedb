java_test(
    name = "isa-explicit-it",
    test_class = "ai.grakn.test.graql.internal.IsaExplicitIT",
    srcs = ["IsaExplicitIT.java"],
    deps = [
        "//test-integration/rule:concurrent-grakn-server",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//grakn-graql"],
    size = "large",
    classpath_resources = ["//test-integration/resources:logback-test"]
)