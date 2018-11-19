java_test(
  name = "casting-it",
  srcs = ["CastingIT.java"],
  test_class = "grakn.core.server.kb.structure.CastingIT",
  deps = [
      "//server",
      "//test-integration/rule:grakn-test-server",
      "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
  ],
  size = "medium",
  classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
  name = "edge-it",
  srcs = ["EdgeIT.java"],
  test_class = "grakn.core.server.kb.structure.EdgeIT",
  deps = [
      "//server",
      "//test-integration/rule:grakn-test-server",
      "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
  ],
  size = "medium",
  classpath_resources = ["//test-integration/resources:logback-test"]
)