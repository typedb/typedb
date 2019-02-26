load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "label-fragment-set-test",
    srcs = ["LabelFragmentSetTest.java"],
    test_class = "grakn.core.graql.gremlin.sets.LabelFragmentSetTest",
    deps = [
        "//concept:concept",
        "//server:server",

        "@graknlabs_graql//java:graql",

        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/mockito:mockito-core"
        ],
    size = "small"
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":label-fragment-set-test",
        ":roleplayer-fragment-set-test",
    ],
)

java_test(
    name = "roleplayer-fragment-set-test",
    test_class = "grakn.core.graql.gremlin.sets.RolePlayerFragmentSetTest",
    srcs = ["RolePlayerFragmentSetTest.java"],
    deps = [
        "//concept:concept",
        "//server:server",

        "@graknlabs_graql//java:graql",

        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
    ],
    size = "small"
)
