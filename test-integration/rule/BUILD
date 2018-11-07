
java_library(
    name = "cassandra-context",
    srcs = ["EmbeddedCassandraContext.java"],
    deps = [
        "//dependencies/maven/artifacts/org/cassandraunit:cassandra-unit",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all",
        "//dependencies/maven/artifacts/org/apache/thrift:libthrift",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/junit:junit"],
    resources = ["//test-integration/resources:cassandra-embedded",],
    visibility = ["//test-integration:__subpackages__"],
)

java_library(
    name = "server-context",
    srcs = ["ServerContext.java"],
    deps = [
        "//server",
        "//grakn-graql",
        "//dependencies/maven/artifacts/junit:junit",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
    ],
    resources = ["//server:grakn-properties"],
    visibility = ["//test-integration:__subpackages__"]
)

java_library(
    name = "concurrent-grakn-server",
    srcs = ["ConcurrentGraknServer.java"],
    deps = [
        "//server",
        "//grakn-graql",
        "//dependencies/maven/artifacts/junit:junit",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/org/cassandraunit:cassandra-unit",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all",
        "//dependencies/maven/artifacts/org/apache/thrift:libthrift",],
    resources = ["//server:grakn-properties"],
    visibility = ["//test-integration:__subpackages__"]
)