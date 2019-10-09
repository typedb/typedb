load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
     name = "attribute-it",
     srcs = ["AttributeIT.java"],
     test_class = "grakn.core.server.grakn.core.kb.concept.AttributeIT",
     deps = [
         "//kb/concept/api:api",
         "//server:server",
         "//test-integration/rule:grakn-test-server",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
         "//dependencies/maven/artifacts/com/google/guava:guava",
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium",
)

java_test(
     name = "attribute-type-it",
     srcs = ["AttributeTypeIT.java"],
     test_class = "grakn.core.server.grakn.core.kb.concept.AttributeTypeIT",
     deps = [
         "//kb/concept/api:api",
         "//server:server",
         "@graknlabs_graql//java:graql",
         "//test-integration/rule:grakn-test-server",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)

java_test(
     name = "concept-it",
     srcs = ["ConceptIT.java"],
     test_class = "grakn.core.server.grakn.core.kb.concept.ConceptIT",
     deps = [
         "//kb/concept/api:api",
         "//server:server",
         "//test-integration/rule:grakn-test-server",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
         "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core"
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)

java_test(
     name = "entity-it",
     srcs = ["EntityIT.java"],
     test_class = "grakn.core.server.grakn.core.kb.concept.EntityIT",
     deps = [
         "//kb/concept/api:api",
         "//server:server",
         "//test-integration/rule:grakn-test-server",

         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)

java_test(
    name = "entity-type-it",
    srcs = ["EntityTypeIT.java"],
    test_class = "grakn.core.server.grakn.core.kb.concept.EntityTypeIT",
    deps = [
        # Internal dependencies
        "//common:common",
        "//kb/concept/api:api",
        "//server:server",
        "//test-integration/rule:grakn-test-server",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core"
    ],
    classpath_resources = ["//test-integration/resources:logback-test"],
    size = "large"
)

java_test(
     name = "relation-it",
     srcs = ["RelationIT.java"],
     test_class = "grakn.core.server.grakn.core.kb.concept.RelationIT",
     deps = [
         "//common:common",
         "//kb/concept/api:api",
         "//server:server",
         "@graknlabs_graql//java:graql",
         "//test-integration/rule:grakn-test-server",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
         "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
         "//dependencies/maven/artifacts/com/google/guava:guava",
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)

java_test(
     name = "relation-type-it",
     srcs = ["RelationTypeIT.java"],
     test_class = "grakn.core.server.grakn.core.kb.concept.RelationTypeIT",
     deps = [
         "//kb/concept/api:api",
         "//server:server",
         "//test-integration/rule:grakn-test-server",

         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)

java_test(
     name = "role-it",
     srcs = ["RoleIT.java"],
     test_class = "grakn.core.server.grakn.core.kb.concept.RoleIT",
     deps = [
         "//kb/concept/api:api",
         "//server:server",
         "//test-integration/rule:grakn-test-server",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)

java_test(
     name = "schema-concept-it",
     srcs = ["SchemaConceptIT.java"],
     test_class = "grakn.core.server.grakn.core.kb.concept.SchemaConceptIT",
     deps = [
         "//common:common",
         "//kb/concept/api:api",
         "//server:server",
         "//test-integration/rule:grakn-test-server",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
         "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)

java_test(
     name = "schema-mutation-it",
     srcs = ["SchemaMutationIT.java"],
     test_class = "grakn.core.server.grakn.core.kb.concept.SchemaMutationIT",
     deps = [
         "//common:common",
         "//kb/concept/api:api",
         "//server:server",
         "//test-integration/rule:grakn-test-server",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
         "//dependencies/maven/artifacts/com/google/guava:guava",
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "large"
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
    ]
)