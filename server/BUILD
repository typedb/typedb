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

load("@rules_antlr//antlr:antlr4.bzl", "antlr4")
load("//dependencies/deployment/maven:rules.bzl", "deploy_maven_jar")


antlr4(
    name = "graql-java",
    srcs = glob(["src/graql/grammar/*.g4"]),
    visitor = True,
    package = "grakn.core.graql.grammar"
)

genrule(
    name = "grakn-version",
    srcs = [
        "templates/util/GraknVersion.java",
        "//:VERSION",
    ],
    cmd = "VERSION=`cat $(location //:VERSION)`;sed -e \"s/VERSION_PLACEHOLDER/$$VERSION/g\" $(location templates/util/GraknVersion.java) >> $@",
    outs = ["util/GraknVersion.java"]
)

java_library(
    name = "server",
    srcs = [
       ":grakn-version",
       ":graql-java",
   ] + glob(["src/**/*.java"]),
    deps = [
        # Grakn Core dependencies
        "//protocol:protocol-java",
        "//util",

        # External dependencies
        "//dependencies/maven/artifacts/com/datastax/cassandra:cassandra-driver-core", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-core",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/core:jackson-databind",
        "//dependencies/maven/artifacts/com/fasterxml/jackson/dataformat:jackson-dataformat-yaml",
        "//dependencies/maven/artifacts/com/google/auto/value:auto-value",
        "//dependencies/maven/artifacts/com/google/code/findbugs:annotations",
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
        "//dependencies/maven/artifacts/org/antlr:antlr4-runtime", # sync version with @antlr4_runtime//jar
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-thrift", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop-annotations", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop-common", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop-mapreduce-client-core", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/apache/spark:spark-core-2-11",
        "//dependencies/maven/artifacts/org/apache/spark:spark-launcher-2-11", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/apache/thrift:libthrift", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/apache/tinkerpop:hadoop-gremlin",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:spark-gremlin",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:tinkergraph-gremlin",
        "//dependencies/maven/artifacts/org/rocksdb:rocksdbjni",
        "//dependencies/maven/artifacts/org/janusgraph:janusgraph-cassandra",
        "//dependencies/maven/artifacts/org/janusgraph:janusgraph-core",
        "//dependencies/maven/artifacts/org/janusgraph:janusgraph-hadoop",
        "//dependencies/maven/artifacts/org/scala-lang:scala-library", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/scala-lang:scalap", # PREVIOUSLY UNDECLARED
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
    resources = glob(["src/resources/*"]),
    tags = ["maven_coordinates=grakn.core:server:{pom_version}"],
    javacopts = ["-XepDisableAllChecks"], # TODO: THIS NEEDS TO BE REMOVED

)

deploy_maven_jar(
    name = "deploy-maven-jar",
    targets = [":server"],
    version_file = "//:VERSION",
)

java_binary(
    name = "server-binary",
    main_class = "grakn.core.server.bootup.GraknBootup",
    runtime_deps = ["//server"],
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

exports_files(
    glob(["conf/**", "services/**"]),
    visibility = ["//visibility:public"]
)

filegroup(
    name = "grakn-properties",
    srcs = ["conf/grakn.properties"],
    visibility = ["//visibility:public"]
)
