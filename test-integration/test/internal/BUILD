java_test(
     name = "validator-it",
     srcs = ["ValidatorIT.java"],
     test_class = "ai.grakn.kb.internal.ValidatorIT",
     deps = [
         "//grakn-graql",
         "//test-integration/rule:cassandra-context",
         "//test-integration/rule:server-context",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
     ],
     size = "medium",
     classpath_resources = ["//test-integration/resources:logback-test"]
 )

java_test(
  name = "validate-global-rules-it",
  srcs = ["ValidateGlobalRulesIT.java"],
  test_class = "ai.grakn.kb.internal.ValidateGlobalRulesIT",
  deps = [
      "//grakn-graql",
      "//test-integration/rule:cassandra-context",
      "//test-integration/rule:server-context"
  ],
  size = "medium",
  classpath_resources = ["//test-integration/resources:logback-test"]
)

java_test(
  name = "grakn-tx-it",
  srcs = ["GraknTxIT.java"],
  test_class = "ai.grakn.kb.internal.GraknTxIT",
  deps = [
      "//grakn-graql",
      "//test-integration/rule:cassandra-context",
      "//test-integration/rule:server-context",
      "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
      "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
  ],
  size = "medium",
  classpath_resources = ["//test-integration/resources:logback-test"]
)