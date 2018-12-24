load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_test(
     name = "validator-it",
     srcs = ["ValidatorIT.java"],
     test_class = "grakn.core.server.kb.ValidatorIT",
     deps = [
         "//server",
         "//common",
         "//test-integration/rule:grakn-test-server",
         "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "medium"
)

checkstyle_test(
 name = "validator-it-checkstyle",
 target = ":validator-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",
 allow_failure = True
)

java_test(
  name = "validate-global-rules-it",
  srcs = ["ValidateGlobalRulesIT.java"],
  test_class = "grakn.core.server.kb.ValidateGlobalRulesIT",
  deps = [
      "//server",
      "//test-integration/rule:grakn-test-server"
  ],
  size = "medium",
  classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
 name = "validate-global-rules-it-checkstyle",
 target = ":validate-global-rules-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",
 allow_failure = True
)

java_test(
  name = "grakn-tx-it",
  srcs = ["TransactionIT.java"],
  test_class = "grakn.core.server.kb.TransactionIT",
  deps = [
      "//server",
      "//common",
      "//test-integration/rule:grakn-test-server",
      "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
      "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
  ],
  size = "medium",
  classpath_resources = ["//test-integration/resources:logback-test"]
)

checkstyle_test(
 name = "grakn-tx-it-checkstyle",
 target = ":grakn-tx-it",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 license = "//config/checkstyle:checkstyle-file-header.txt",
 allow_failure = True
)