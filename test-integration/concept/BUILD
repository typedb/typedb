load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
    name = "attribute-it",
    size = "medium",
    srcs = ["AttributeIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.concept.AttributeIT",
    deps = [
        "//concept/impl:impl",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",
    ],
)

java_test(
    name = "attribute-type-it",
    size = "medium",
    srcs = ["AttributeTypeIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.concept.AttributeTypeIT",
    deps = [
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "concept-it",
    size = "medium",
    srcs = ["ConceptIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.concept.ConceptIT",
    deps = [
        "//core",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//kb/concept/structure",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:concept-downcasting",
    ],
)

java_test(
    name = "entity-it",
    size = "medium",
    srcs = ["EntityIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.concept.EntityIT",
    deps = [
        "//core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//kb/concept/structure",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:concept-downcasting",
    ],
)

java_test(
    name = "entity-type-it",
    size = "large",
    srcs = ["EntityTypeIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.concept.EntityTypeIT",
    deps = [
        # Internal dependencies
        "//common:common",
        "//kb/server",
        "//core",
        "//kb/concept/structure",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",
        "//test-integration/util:concept-downcasting",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
    ],
)

java_test(
    name = "relation-it",
    size = "medium",
    srcs = ["RelationIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.concept.RelationIT",
    deps = [
        "//common:common",
        "//concept/answer",
        "//core",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//kb/concept/manager",
        "//test-integration/rule:grakn-test-server",
        "@graknlabs_graql//java:graql",
    ],
)

java_test(
    name = "relation-type-it",
    size = "medium",
    srcs = ["RelationTypeIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.concept.RelationTypeIT",
    deps = [
        "//core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",
    ],
)

java_test(
    name = "role-it",
    size = "medium",
    srcs = ["RoleIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.concept.RoleIT",
    deps = [
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",
    ],
)

java_test(
    name = "schema-concept-it",
    size = "medium",
    srcs = ["SchemaConceptIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.concept.SchemaConceptIT",
    deps = [
        "//common:common",
        "//concept/impl:impl",
        "//core",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//kb/concept/structure",
        "//test-integration/rule:grakn-test-server",
    ],
)

java_test(
    name = "schema-mutation-it",
    size = "large",
    srcs = ["SchemaMutationIT.java"],
    classpath_resources = ["//test-integration/resources:logback-test"],
    test_class = "grakn.core.concept.SchemaMutationIT",
    deps = [
        "//common:common",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//kb/server",
        "//kb/concept/api",
        "//test-integration/rule:grakn-test-server",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":attribute-it",
        ":attribute-type-it",
        ":concept-it",
        ":entity-it",
        ":entity-type-it",
        ":relation-it",
        ":relation-type-it",
        ":role-it",
        ":schema-concept-it",
        ":schema-mutation-it",
    ],
)
