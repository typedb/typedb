java_test(
     name = "validator-it",
     srcs = ["ValidatorIT.java"],
     test_class = "grakn.core.server.kb.internal.ValidatorIT",
     deps = [
         "//server",
         "//util",
         "//test-integration/rule:concurrent-grakn-server",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)

java_test(
  name = "validate-global-rules-it",
  srcs = ["ValidateGlobalRulesIT.java"],
  test_class = "grakn.core.server.kb.internal.ValidateGlobalRulesIT",
  deps = [
      "//server",
      "//test-integration/rule:concurrent-grakn-server"
  ],
  size = "medium",
  classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
  name = "grakn-tx-it",
  srcs = ["TransactionIT.java"],
  test_class = "grakn.core.server.kb.internal.TransactionIT",
  deps = [
      "//server",
      "//util",
      "//test-integration/rule:concurrent-grakn-server",
      "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
      "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
  ],
  size = "medium",
  classpath_resources = ["//test-integration/resources:logback-test"]
)