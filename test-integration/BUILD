java_library(
    name = "test-integration",
    srcs = glob(["src/test/java/ai/**/*.java"])
)

java_test(
    name = "AttributeDeduplicatorIT",
    srcs = ["src/test/java/ai/grakn/server/attribute/deduplicator/AttributeDeduplicatorIT.java"],
    test_class = "ai.grakn.test.server.attribute.deduplicator.AttributeDeduplicatorIT",
    deps = [
        "//server",
        ":CassandraContext",
        "//grakn-graql",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"],
    resources = ["//server:grakn-properties"],
    size = "medium"
)

java_test(
    name = "ServerStartIT",
    srcs = ["src/test/java/ai/grakn/server/ServerStartIT.java"],
    test_class = "ai.grakn.test.server.ServerStartIT",
    deps = [
        "//server",
        ":CassandraContext",
        ":ServerContext",
        "//grakn-graql",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library"],
    size = "medium"
)

java_library(
    name = "CassandraContext",
    srcs = ["src/test/java/ai/grakn/rule/EmbeddedCassandraContext.java"],
    deps = [
        "//dependencies/maven/artifacts/org/cassandraunit:cassandra-unit",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all",
        "//dependencies/maven/artifacts/org/apache/thrift:libthrift",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/junit:junit"],
    resources = [ "src/test/resources/cassandra-embedded.yaml",
                 "src/test/resources/logback.xml"],
)

java_library(
    name = "ServerContext",
    srcs = ["src/test/java/ai/grakn/rule/ServerContext.java", "src/test/java/ai/grakn/util/GraknTestUtil.java"],
    deps = [
        "//server",
        "//grakn-graql",
        "//dependencies/maven/artifacts/junit:junit",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/sparkjava:spark-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/com/jayway/restassured:rest-assured",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/com/codahale/metrics:metrics-core"],
    resources = ["//server:grakn-properties"]
)