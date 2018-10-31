java_test(
     name = "attribute-it",
     srcs = ["AttributeIT.java"],
     test_class = "ai.grakn.kb.internal.concept.AttributeIT",
     deps = [
         "//grakn-graql",
         "//test-integration/rule:cassandra-context",
         "//test-integration/rule:server-context",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
         "//dependencies/maven/artifacts/com/google/guava:guava",
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium",
)
java_test(
     name = "attribute-type-it",
     srcs = ["AttributeTypeIT.java"],
     test_class = "ai.grakn.kb.internal.concept.AttributeTypeIT",
     deps = [
         "//grakn-graql",
         "//test-integration/rule:cassandra-context",
         "//test-integration/rule:server-context",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)
java_test(
     name = "concept-it",
     srcs = ["ConceptIT.java"],
     test_class = "ai.grakn.kb.internal.concept.ConceptIT",
     deps = [
         "//grakn-graql",
         "//test-integration/rule:cassandra-context",
         "//test-integration/rule:server-context",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
         "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core"
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)
java_test(
     name = "entity-it",
     srcs = ["EntityIT.java"],
     test_class = "ai.grakn.kb.internal.concept.EntityIT",
     deps = [
         "//grakn-graql",
         "//test-integration/rule:cassandra-context",
         "//test-integration/rule:server-context",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)
java_test(
     name = "entity-type-it",
     srcs = ["EntityTypeIT.java"],
     test_class = "ai.grakn.kb.internal.concept.EntityTypeIT",
     deps = [
         "//grakn-graql",
         "//test-integration/rule:cassandra-context",
         "//test-integration/rule:server-context",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
         "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core"
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium",
     javacopts = ["-XepDisableAllChecks"], # TODO: THIS NEEDS TO BE REMOVED
     jvm_flags = ["-XepDisableAllChecks"]
)
