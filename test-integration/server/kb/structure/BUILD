load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
  name = "casting-it",
  srcs = ["CastingIT.java"],
  test_class = "grakn.core.server.kb.structure.CastingIT",
  deps = [
      "//kb/concept/api:api",
      "//server:server",
      "//kb/concept/structure:structure",
      "//kb:kb",
      "//concept/impl:concept",
      "//test-integration/rule:grakn-test-server",
      "//test-integration/util:concept-downcasting",

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
      "//kb/concept/api:api",
      "//server:server",
      "//kb:kb",
      "//kb/concept/structure:structure",
      "//concept/structure:structure",
      "//concept/impl:concept",
      "//core:core",
      "//kb/concept/manager:manager",
      "//common",
      "//test-integration/rule:grakn-test-server",
      "//dependencies/maven/artifacts/com/google/guava:guava",
      "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
      "//dependencies/maven/artifacts/org/janusgraph:janusgraph-core",
      "//dependencies/maven/artifacts/org/mockito:mockito-core",
      "//test-integration/util:concept-downcasting"
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