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

package(default_visibility = ["//visibility:public"])

load("@org_pubref_rules_proto//java:compile.bzl", "java_grpc_compile")
java_grpc_compile(
    name = "client-java-proto",
    deps = [
        "//protocol/session:session-proto",
        "//protocol/session:answer-proto",
        "//protocol/session:concept-proto",
        "//protocol/keyspace:keyspace-proto",
    ]
)

java_library(
    name = "client-java",
    srcs = [":client-java-proto"] + glob(["src/main/java/**/*.java"]),
    deps = [
        # Grakn Core dependencies
        "//grakn-graql:grakn-graql",

        # External dependencies
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/ch/qos/logback:logback-core",
        "//dependencies/maven/artifacts/com/codahale/metrics:metrics-core",
        "//dependencies/maven/artifacts/com/github/rholder:guava-retrying",
        "//dependencies/maven/artifacts/com/google/auto/value:auto-value",
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/com/netflix/hystrix:hystrix-core",
        "//dependencies/maven/artifacts/com/sun/jersey:jersey-client",
        "//dependencies/maven/artifacts/io/dropwizard/metrics:metrics-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-netty",
        "//dependencies/maven/artifacts/io/grpc:grpc-protobuf",
        "//dependencies/maven/artifacts/io/grpc:grpc-stub",
        "//dependencies/maven/artifacts/io/reactivex:rxjava", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/io/netty:netty-all",
        "//dependencies/maven/artifacts/io/zipkin/brave:brave",
        "//dependencies/maven/artifacts/io/zipkin/brave:brave-instrumentation-grpc",
        "//dependencies/maven/artifacts/io/zipkin/reporter2:zipkin-sender-urlconnection",
        "//dependencies/maven/artifacts/io/zipkin/reporter2:zipkin-reporter", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/io/zipkin/zipkin2:zipkin", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/javax/ws/rs:jsr311-api",
        "//dependencies/maven/artifacts/net/jodah:failsafe",
        "//dependencies/maven/artifacts/org/sharegov:mjson", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ]
)

java_test(
    name = "transaction-test",
    srcs = ["src/test/java/ai/grakn/client/TransactionTest.java","src/test/java/ai/grakn/client/ServerRPCMock.java"],
    test_class = "ai.grakn.client.TransactionTest",
    deps = [
        ":client-java",
        "//grakn-graql",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-library",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-testing",
        "//dependencies/maven/artifacts/io/grpc:grpc-stub",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/com/google/guava:guava",
    ],
    size = "small",
)

test_suite(
    name = "test-unit",
    tags = ["small"],
)
