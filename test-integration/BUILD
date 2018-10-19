java_library(
    name = "test-integration",
    srcs = glob(["src/test/java/ai/**/*.java"]),
   deps = [
            # External dependencies
#            "//dependencies/maven/artifacts/org/cassandraunit:cassandra-unit",
        ]
)

java_test(
    name = "AttributeDeduplicatorIT",
    srcs = ["src/test/java/ai/grakn/engine/attribute/deduplicator/AttributeDeduplicatorIT.java"],
    test_class = "ai.grakn.test.engine.attribute.deduplicator.AttributeDeduplicatorIT",
    deps = [
        "//server",
        ":EmbeddedCassandra",
        "//grakn-graql",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"],
    resources = ["//server:grakn-properties"],
)

java_library(
    name = "EmbeddedCassandra",
    srcs = ["src/test/java/ai/grakn/rule/EmbeddedCassandraContext.java"],
    deps = [
        "//dependencies/maven/artifacts/org/cassandraunit:cassandra-unit",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all",
        "//dependencies/maven/artifacts/org/apache/thrift:libthrift",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/junit:junit"],
    resources = ["//server:grakn-properties",
                 "src/test/resources/cassandra-embedded.yaml",
                 "src/test/resources/logback.xml"],
)