load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
    name = "atomic-type-inference-it",
    srcs = ["AtomicTypeInferenceIT.java"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicTypeInferenceIT",
    deps = [
            "//server",
            "//test-integration/rule:grakn-test-server",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//test-integration/util:graql-test-util"
        ],
    resources = ["//test-integration/graql/reasoner/resources:type-inference"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
 name = "atomic-type-inference-it-checkstyle",
 target = ":atomic-type-inference-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "atomic-unification-it",
    srcs = ["AtomicUnificationIT.java"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicUnificationIT",
    deps = [
            "//server",
                        "//test-integration/rule:grakn-test-server",

            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//test-integration/util:graql-test-util"
        ],
    resources = ["//test-integration/graql/reasoner/resources:generic-schema"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
 name = "atomic-unification-it-checkstyle",
 target = ":atomic-unification-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "atomic-rule-applicability-it",
    srcs = ["AtomicRuleApplicabilityIT.java"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicRuleApplicabilityIT",
    deps = [
            "//server",
            "//test-integration/rule:grakn-test-server",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava",
            "//test-integration/util:graql-test-util"
        ],
    resources = ["//test-integration/graql/reasoner/resources:resource-applicability",
                 "//test-integration/graql/reasoner/resources:rule-applicability",
                 "//test-integration/graql/reasoner/resources:reified-resource-applicability"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
 name = "atomic-rule-applicability-it-checkstyle",
 target = ":atomic-rule-applicability-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "atomic-role-inference-it",
    srcs = ["AtomicRoleInferenceIT.java"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicRoleInferenceIT",
    deps = [
            "//server",
            "//test-integration/rule:grakn-test-server",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava"
        ],
    resources = ["//test-integration/graql/reasoner/resources:role-inference",
                     "//test-integration/graql/reasoner/resources:rule-applicability",
                     "//test-integration/graql/reasoner/resources:generic-schema"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
 name = "atomic-role-inference-it-checkstyle",
 target = ":atomic-role-inference-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)

java_test(
    name = "atomic-equivalence-it",
    srcs = ["AtomicEquivalenceIT.java"],
    test_class = "grakn.core.graql.reasoner.atomic.AtomicEquivalenceIT",
    deps = [
            "//server",
            "//test-integration/rule:grakn-test-server",
            "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
            "//dependencies/maven/artifacts/com/google/guava:guava"
        ],
    resources = ["//test-integration/graql/reasoner/resources:generic-schema"],
    size = "medium",
    classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
 name = "atomic-equivalence-it-checkstyle",
 target = ":atomic-equivalence-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",

)
