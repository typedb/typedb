load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "in-plays-fragment-test",
    size = "small",
    srcs = ["InPlaysFragmentTest.java"],
    test_class = "grakn.core.graql.gremlin.fragment.InPlaysFragmentTest",
    deps = [
        "//concept",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "isa-explicit-test",
    size = "small",
    srcs = ["IsaExplicitTest.java"],
    test_class = "grakn.core.graql.gremlin.fragment.IsaExplicitTest",
    deps = [
        "//concept",
        "//server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "out-plays-fragment-test",
    size = "small",
    srcs = ["OutPlaysFragmentTest.java"],
    test_class = "grakn.core.graql.gremlin.fragment.OutPlaysFragmentTest",
    deps = [
        "//concept",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//server",
        "@graknlabs_graql//java:graql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":in-plays-fragment-test",
        ":isa-explicit-test",
        ":out-plays-fragment-test",
    ],
)
