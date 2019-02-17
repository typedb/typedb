load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "in-plays-fragment-test",
    srcs = ["InPlaysFragmentTest.java"],
    test_class = "grakn.core.graql.internal.gremlin.fragment.InPlaysFragmentTest",
    deps = [
        "@graknlabs_graql//java:graql",
        "//server:server",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core"
    ],
    size = "small"
)

java_test(
    name = "isa-explicit-test",
    srcs = ["IsaExplicitTest.java"],
    test_class = "grakn.core.graql.internal.gremlin.fragment.IsaExplicitTest",
    deps = [
        "@graknlabs_graql//java:graql",
        "//server:server",
    ],
    size = "small"
)

java_test(
     name = "out-plays-fragment-test",
     srcs = ["OutPlaysFragmentTest.java"],
     test_class = "grakn.core.graql.internal.gremlin.fragment.OutPlaysFragmentTest",
     deps = [
         "@graknlabs_graql//java:graql",
         "//server:server",
         "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
         "//dependencies/maven/artifacts/com/google/guava:guava"
         ],
     size = "small"
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":in-plays-fragment-test",
        ":isa-explicit-test",
        ":out-plays-fragment-test"
    ],
)
