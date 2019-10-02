load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
  name = "casting-it",
  srcs = ["CastingIT.java"],
  test_class = "grakn.core.server.kb.structure.CastingIT",
  deps = [
      "//concept:concept",
      "//server:server",
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
      "//concept:concept",
      "//server:server",
      "//common",
      "//test-integration/rule:grakn-test-server",

      "//dependencies/maven/artifacts/com/google/guava:guava",
      "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
      "//dependencies/maven/artifacts/org/janusgraph:janusgraph-core",
      "//dependencies/maven/artifacts/org/mockito:mockito-core"
  ],
  size = "medium",
  classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":casting-it",
        ":edge-it"
    ]
)