load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "in-plays-fragment-test",
    srcs = ["InPlaysFragmentTest.java"],
    test_class = "grakn.core.graql.internal.gremlin.fragment.InPlaysFragmentTest",
    deps = [
        "//server",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core"
    ],
    size = "small"
)

checkstyle_test(
 name = "in-plays-fragment-test-checkstyle",
 target = ":in-plays-fragment-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_test(
    name = "isa-explicit-test",
    srcs = ["IsaExplicitTest.java"],
    test_class = "grakn.core.graql.internal.gremlin.fragment.IsaExplicitTest",
    deps = ["//server"],
    size = "small"
)

checkstyle_test(
 name = "isa-explicit-test-checkstyle",
 target = ":isa-explicit-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)

java_test(
     name = "out-plays-fragment-test",
     srcs = ["OutPlaysFragmentTest.java"],
     test_class = "grakn.core.graql.internal.gremlin.fragment.OutPlaysFragmentTest",
     deps = [
         "//server",
         "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
         "//dependencies/maven/artifacts/com/google/guava:guava"
         ],
     size = "small"
)

checkstyle_test(
 name = "out-plays-fragment-test-checkstyle",
 target = ":out-plays-fragment-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)