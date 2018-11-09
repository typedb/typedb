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

load("//dependencies/deployment/maven:rules.bzl", "deploy_maven_jar")

java_library(
    name = "server",
    srcs = glob(["src/**/*.java"]),
    deps = [
        # Grakn Core dependencies
        "//grakn-graql:grakn-graql",
        "//protocol:protocol-java",

        # External dependencies
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-core",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-databind",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/dataformat:jackson-dataformat-yaml",
        "//dependencies/maven/artifacts/com/google/auto/value:auto-value",
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/commons-configuration:commons-configuration", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/commons-io:commons-io", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/commons-cli:commons-cli", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-stub",
        "//dependencies/maven/artifacts/io/zipkin/brave:brave",
        "//dependencies/maven/artifacts/io/zipkin/reporter2:zipkin-sender-urlconnection",
        "//dependencies/maven/artifacts/io/zipkin/reporter2:zipkin-reporter", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/io/zipkin/zipkin2:zipkin", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/rocksdb:rocksdbjni",
        "//dependencies/maven/artifacts/org/sharegov:mjson", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/org/zeroturnaround:zt-exec",
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/ch/qos/logback:logback-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-netty",
        "//dependencies/maven/artifacts/io/netty:netty-all",
        "//dependencies/maven/artifacts/javax/servlet:javax-servlet-api", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/codehaus/janino:janino",
    ],
    visibility = ["//visibility:public"],
    tags = ["maven_coordinates=ai.grakn:core.server:{pom_version}"],
)

deploy_maven_jar(
    name = "deploy-maven-jar",
    targets = [":server"],
    version_file = "//:VERSION",
)

java_binary(
    name = "server-binary",
    main_class = "ai.grakn.core.server.bootup.GraknBootup",
    runtime_deps = ["//server:server"],
    visibility = ["//visibility:public"]
)

genrule(
    name = "distribution",
    srcs = glob(["conf/**", "services/**"]) + [
        ":server-binary_deploy.jar",
        "//:grakn",
    ],
    outs = ["//server:dist/grakn-core-server.zip"],
    cmd  = "$(location distribution.sh) $(location //server:dist/grakn-core-server.zip) $(location //:grakn) $(location services/grakn/grakn-core-ascii.txt) $(location server-binary_deploy.jar) $(location //server:conf/grakn.properties) $(location //server:conf/logback.xml) $(location //server:services/cassandra/logback.xml) $(location //server:services/cassandra/cassandra.yaml)",
    tools = ["distribution.sh"]
)

java_test(
    name = "process-wide-lock-provider-test",
    test_class = "ai.grakn.core.server.lock.ProcessWideLockProviderTest",
    srcs = ["test/lock/ProcessWideLockProviderTest.java"],
    deps = [":server"],
    size = "small"
)

java_test(
    name = "rocks-db-queue-test",
    test_class = "ai.grakn.core.server.deduplicator.RocksDbQueueTest",
    srcs = ["test/deduplicator/RocksDbQueueTest.java"],
    deps = [
        ":server",
        "//grakn-graql",
        "//dependencies/maven/artifacts/commons-io:commons-io",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
    ],
    size = "small",
    flaky = True
)

java_test(
    name = "grakn-config-test",
    test_class = "ai.grakn.core.server.config.GraknConfigTest",
    srcs = ["test/config/GraknConfigTest.java"],
    deps = [":server", "//grakn-graql"],
    resources = ["test/resources/inmemory-graph.properties"],
    size = "small"
)

test_suite(
    name = "test-unit",
    tags = ["small"],
)

exports_files(
    glob(["conf/**", "services/**"]),
    visibility = ["//visibility:public"]
)

filegroup(
    name = "grakn-properties",
    srcs = ["conf/grakn.properties"],
    visibility = ["//visibility:public"]
)
