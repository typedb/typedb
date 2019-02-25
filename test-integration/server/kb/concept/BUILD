load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
     name = "attribute-it",
     srcs = ["AttributeIT.java"],
     test_class = "grakn.core.server.kb.concept.AttributeIT",
     deps = [
         "//concept:concept",
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
     test_class = "grakn.core.server.kb.concept.AttributeTypeIT",
     deps = [
         "//concept:concept",
         "//server:server",
         "//test-integration/rule:grakn-test-server",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)

java_test(
     name = "concept-it",
     srcs = ["ConceptIT.java"],
     test_class = "grakn.core.server.kb.concept.ConceptIT",
     deps = [
         "//concept:concept",
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
     test_class = "grakn.core.server.kb.concept.EntityIT",
     deps = [
         "//concept:concept",
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
     test_class = "grakn.core.server.kb.concept.EntityTypeIT",
     deps = [
         "//common:common",
         "//concept:concept",
         "//server:server",
         "//test-integration/rule:grakn-test-server",

         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
         "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core"
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "large"
)

java_test(
     name = "relationship-it",
     srcs = ["RelationIT.java"],
     test_class = "grakn.core.server.kb.concept.RelationIT",
     deps = [
         "//common:common",
         "//concept:concept",
         "//server:server",
         "//test-integration/rule:grakn-test-server",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
         "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
         "//dependencies/maven/artifacts/com/google/guava:guava",
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)

java_test(
     name = "relationship-type-it",
     srcs = ["RelationTypeIT.java"],
     test_class = "grakn.core.server.kb.concept.RelationTypeIT",
     deps = [
         "//concept:concept",
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
     test_class = "grakn.core.server.kb.concept.RoleIT",
     deps = [
         "//concept:concept",
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
     test_class = "grakn.core.server.kb.concept.SchemaConceptIT",
     deps = [
         "//common:common",
         "//concept:concept",
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
     test_class = "grakn.core.server.kb.concept.SchemaMutationIT",
     deps = [
         "//common:common",
         "//concept:concept",
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
        ":relationship-it",
        ":relationship-type-it",
        ":role-it",
        ":schema-concept-it",
        ":schema-mutation-it",
    ]
)