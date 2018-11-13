java_test(
  name = "casting-it",
  srcs = ["CastingIT.java"],
  test_class = "grakn.core.server.kb.internal.structure.CastingIT",
  deps = [
      "//grakn-graql",
      "//test-integration/rule:concurrent-grakn-server",
      "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
  ],
  size = "medium",
  classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
  name = "edge-it",
  srcs = ["EdgeIT.java"],
  test_class = "grakn.core.server.kb.internal.structure.EdgeIT",
  deps = [
      "//grakn-graql",
      "//test-integration/rule:concurrent-grakn-server",
      "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
  ],
  size = "medium",
  classpath_resources = ["//test-integration/resources:logback-test"]
)