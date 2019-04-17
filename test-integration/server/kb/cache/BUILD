load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_test(
     name = "concept-caches-it",
     srcs = ["ConceptCachesIT.java"],
     test_class = "grakn.core.server.kb.cache.ConceptCachesIT",
     deps = [
         "//concept:concept",
         "//server:server",
         "//test-integration/rule:grakn-test-server",
     ],
     classpath_resources = ["//test-integration/resources:logback-test"],
     size = "small",
)


checkstyle_test(
    name = "checkstyle",
    targets = [
        ":concept-caches-it",
    ]
)