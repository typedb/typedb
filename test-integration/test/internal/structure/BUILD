java_test(
  name = "casting-it",
  srcs = ["CastingIT.java"],
  test_class = "ai.grakn.kb.internal.structure.CastingIT",
  deps = [
      "//grakn-graql",
      "//test-integration/rule:cassandra-context",
      "//test-integration/rule:server-context",
      "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
  ],
  size = "medium",
  classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
  name = "edge-it",
  srcs = ["EdgeIT.java"],
  test_class = "ai.grakn.kb.internal.structure.EdgeIT",
  deps = [
      "//grakn-graql",
      "//test-integration/rule:cassandra-context",
      "//test-integration/rule:server-context",
      "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
  ],
  size = "medium",
  classpath_resources = ["//test-integration/resources:logback-test"]
)