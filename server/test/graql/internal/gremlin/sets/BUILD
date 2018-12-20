load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "label-fragment-set-test",
    srcs = ["LabelFragmentSetTest.java"],
    test_class = "grakn.core.graql.internal.gremlin.sets.LabelFragmentSetTest",
    deps = [
        "//server",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/mockito:mockito-core"
        ],
    size = "small"
)

checkstyle_test(
 name = "label-fragment-set-test-checkstyle",
 target = ":label-fragment-set-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",
 allow_failure = True
)

java_test(
    name = "roleplayer-fragment-set-test",
    test_class = "grakn.core.graql.internal.gremlin.sets.RolePlayerFragmentSetTest",
    srcs = ["RolePlayerFragmentSetTest.java"],
    deps = [
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//server"],
    size = "small"
)

checkstyle_test(
 name = "roleplayer-fragment-set-test-checkstyle",
 target = ":roleplayer-fragment-set-test",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",
 allow_failure = True
)