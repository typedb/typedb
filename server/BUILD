#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2018 Grakn Labs Ltd
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

load("@org_pubref_rules_proto//java:compile.bzl", "java_grpc_compile")
java_grpc_compile(
    name = "server-java-proto",
    deps = [
        "//protocol/session:session-proto",
        "//protocol/session:answer-proto",
        "//protocol/session:concept-proto",
        "//protocol/keyspace:keyspace-proto",
    ]
)

java_library(
    name = "server",
    srcs = [":server-java-proto"] + glob(["src/main/java/**/*.java"]),
    deps = [
        # Grakn Core dependencies
        "//grakn-graql:grakn-graql",

        # External dependencies
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/ch/qos/logback:logback-core",
        "//dependencies/maven/artifacts/com/boundary:high-scale-lib",
        "//dependencies/maven/artifacts/com/codahale/metrics:metrics-core",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-annotations",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-core",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-databind",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/dataformat:jackson-dataformat-yaml",
        "//dependencies/maven/artifacts/com/github/rholder:guava-retrying",
        "//dependencies/maven/artifacts/com/google/auto/value:auto-value",
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/com/jayway/restassured:rest-assured",
        "//dependencies/maven/artifacts/com/sparkjava:spark-core",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-core", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/commons-configuration:commons-configuration", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/commons-io:commons-io", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/commons-cli:commons-cli", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/io/airlift:airline",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-core",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-json", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-servlets",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-netty",
        "//dependencies/maven/artifacts/io/grpc:grpc-protobuf",
        "//dependencies/maven/artifacts/io/grpc:grpc-stub",
        "//dependencies/maven/artifacts/io/netty:netty-all",
        "//dependencies/maven/artifacts/io/prometheus:simpleclient", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/io/prometheus:simpleclient-common", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/io/prometheus:simpleclient-dropwizard", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/io/prometheus:simpleclient-servlet",
        "//dependencies/maven/artifacts/io/zipkin/brave:brave",
        "//dependencies/maven/artifacts/io/zipkin/brave:brave-instrumentation-grpc",
        "//dependencies/maven/artifacts/io/zipkin/reporter2:zipkin-sender-urlconnection",
        "//dependencies/maven/artifacts/io/zipkin/reporter2:zipkin-reporter", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/io/zipkin/zipkin2:zipkin", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/javax/servlet:javax-servlet-api", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/jline:jline",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/apache/httpcomponents:httpcore", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/codehaus/janino:janino",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//dependencies/maven/artifacts/org/rocksdb:rocksdbjni",
        "//dependencies/maven/artifacts/org/sharegov:mjson", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
        "//dependencies/maven/artifacts/ro/fortsoft/pf4j:pf4j",
    ],
    resources = glob(["src/main/resources/*"]),
    visibility = ["//visibility:public"]

)

java_test(
    name = "process-wide-lock-provider-test",
    test_class = "ai.grakn.engine.lock.ProcessWideLockProviderTest",
    srcs = ["src/test/java/ai/grakn/engine/lock/ProcessWideLockProviderTest.java"],
    deps = [":server"],
    size = "small"
)

java_test(
    name = "rocks-db-queue-test",
    test_class = "ai.grakn.engine.attribute.deduplicator.RocksDbQueueTest",
    srcs = ["src/test/java/ai/grakn/engine/attribute/deduplicator/RocksDbQueueTest.java"],
    deps = [
        ":server",
        "//grakn-graql",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
    ],
    size = "small"
)

java_test(
    name = "grakn-config-test",
    test_class = "ai.grakn.engine.config.GraknConfigTest",
    srcs = ["src/test/java/ai/grakn/engine/config/GraknConfigTest.java"],
    deps = [":server", "//grakn-graql"],
    resources = ["src/test/resources/inmemory-graph.properties"],
    size = "small"
)

java_test(
    name = "requests-test",
    test_class = "ai.grakn.engine.controller.util.RequestsTest",
    srcs = ["src/test/java/ai/grakn/engine/controller/util/RequestsTest.java"],
     deps = [
        ":server",
        "//grakn-graql",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/sharegov:mjson"
        ],
    size = "small"
)

test_suite(
    name = "test-unit",
    tags = ["small"],
)

exports_files(
    ["conf/logback.xml", "conf/grakn.properties", "src/services/cassandra/logback.xml", "src/services/cassandra/cassandra.yaml"],
    visibility = ["//visibility:public"]
)

filegroup(
    name = "grakn-properties",
    srcs = ["conf/grakn.properties"],
    visibility = ["//visibility:public"]
)
