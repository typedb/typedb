load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

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

checkstyle_test(
 name = "casting-it-checkstyle",
 target = ":casting-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

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

checkstyle_test(
 name = "edge-it-checkstyle",
 target = ":edge-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],

)