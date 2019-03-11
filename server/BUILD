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

load("//dependencies/maven:rules.bzl", "assemble_maven", "deploy_maven")
load("@graknlabs_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@graknlabs_bazel_distribution//common:rules.bzl", "assemble_targz", "java_deps", "assemble_zip")
load("@graknlabs_bazel_distribution//rpm:rules.bzl", "assemble_rpm", "deploy_rpm")
load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

exports_files(
    glob(["conf/**", "services/**"]),
    visibility = ["//visibility:public"]
)

java_library(
    name = "server",
    srcs = glob(["src/**/*.java"]),
    deps = [
        # Internal dependencies
        "//api:api",
        "//common:common",
        "//concept:concept",
        "//protocol:protocol-java",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/grakn/benchmark:lib",
        "//dependencies/maven/artifacts/com/datastax/cassandra:cassandra-driver-core", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/com/google/auto/value:auto-value",
        "//dependencies/maven/artifacts/com/google/code/findbugs:annotations",
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/commons-cli:commons-cli", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/commons-configuration:commons-configuration", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/commons-lang:commons-lang",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-stub",
        "//dependencies/maven/artifacts/io/zipkin/brave:brave",
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
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/ch/qos/logback:logback-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-netty",
        "//dependencies/maven/artifacts/io/netty:netty-all",
        "//dependencies/maven/artifacts/javax/servlet:javax-servlet-api", # PREVIOUSLY UNDECLARED
        "//dependencies/maven/artifacts/org/codehaus/janino:janino",
    ],
    resources = ["LICENSE"] + glob(["resources/*"]),
    resource_strip_prefix = "server",
    tags = ["maven_coordinates=grakn.core:server:{pom_version}"],
    javacopts = ["-XepDisableAllChecks"], # TODO: THIS NEEDS TO BE REMOVED

)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":server"
    ],
)

java_binary(
    name = "server-binary",
    main_class = "grakn.core.daemon.GraknDaemon",
    runtime_deps = [
        "//daemon",
        "//server"
    ],
    visibility = ["//:__pkg__"],
)

java_deps(
    name = "server-deps",
    target = ":server-binary",
    java_deps_root = "server/services/lib/",
    visibility = ["//:__pkg__"]
)

assemble_targz(
    name = "assemble-linux-targz",
    output_filename = "grakn-core-server",
    targets = [":server-deps", "//bin:assemble-bash-targz"],
    additional_files = {
        "//server:conf/logback.xml": "conf/logback.xml",
        "//server:conf/grakn.properties": "conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt"
    },
    empty_directories = [
        "server/db/cassandra",
        "server/db/queue"
    ],
    permissions = {
      "server/services/cassandra/cassandra.yaml": "0777",
      "server/db/cassandra": "0777",
      "server/db/queue": "0777",
    }
)

assemble_zip(
    name = "assemble-mac-zip",
    output_filename = "grakn-core-server-mac",
    targets = [":server-deps", "//bin:assemble-bash-targz"],
    additional_files = {
        "//server:conf/logback.xml": "conf/logback.xml",
        "//server:conf/grakn.properties": "conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt"
    },
    empty_directories = [
        "server/db/cassandra",
        "server/db/queue"
    ],
    permissions = {
      "server/services/cassandra/cassandra.yaml": "0777",
      "server/db/cassandra": "0777",
      "server/db/queue": "0777",
    }
)

assemble_zip(
    name = "assemble-windows-zip",
    output_filename = "grakn-core-server-windows",
    targets = [":server-deps", "//bin:assemble-bat-targz"],
    additional_files = {
        "//server:conf/logback.xml": "conf/logback.xml",
        "//server:conf/grakn.properties": "conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt"
    },
    empty_directories = [
        "server/db/cassandra",
        "server/db/queue"
    ],
    permissions = {
      "server/services/cassandra/cassandra.yaml": "0777",
      "server/db/cassandra": "0777",
      "server/db/queue": "0777",
    }
)

assemble_maven(
    name = "assemble-maven",
    target = ":server",
    package = "server",
)

deploy_maven(
    name = "deploy-maven",
    target = ":assemble-maven"
)

assemble_apt(
    name = "assemble-apt",
    package_name = "grakn-core-server",
    maintainer = "Grakn Labs <community@grakn.ai>",
    description = "Grakn Core (server)",
    version_file = "//:VERSION",
    depends = [
      "openjdk-8-jre",
      "grakn-core-bin"
    ],
    archives = [":server-deps"],
    installation_dir = "/opt/grakn/core/",
    files = {
        "//server:conf/logback.xml": "conf/logback.xml",
        "//server:conf/grakn.properties": "conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt"
    },
    empty_dirs = [
        "opt/grakn/core/server/services/lib/",
        "var/lib/grakn/db/queue",
        "var/lib/grakn/db/cassandra"
     ],
    permissions = {
        "var/lib/grakn/db/queue": "0777",
        "var/lib/grakn/db/cassandra": "0777",
    },
    symlinks = {
        "opt/grakn/core/server/db/": "/var/lib/grakn/db/",
    },
)

deploy_apt(
    name = "deploy-apt",
    target = ":assemble-apt",
    deployment_properties = "@graknlabs_build_tools//:deployment.properties",
)

assemble_rpm(
    name = "assemble-rpm",
    package_name = "grakn-core-server",
    installation_dir = "/opt/grakn/core/",
    version_file = "//:VERSION",
    spec_file = "//config/rpm:grakn-core-server.spec",
    archives = [":server-deps"],
    empty_dirs = [
        "opt/grakn/core/server/services/lib/",
        "var/lib/grakn/db/queue",
        "var/lib/grakn/db/cassandra"
     ],
    files = {
        "//server:conf/logback.xml": "conf/logback.xml",
        "//server:conf/grakn.properties": "conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt"
    },
    permissions = {
        "services/cassandra/cassandra.yaml": "0777",
        "var/lib/grakn/db/queue": "0777",
        "var/lib/grakn/db/cassandra": "0777",
    },
    symlinks = {
        "opt/grakn/core/server/db/": "/var/lib/grakn/db/",
    },
)

#deploy_rpm(
#    name = "deploy-rpm",
#    target = ":assemble-rpm",
#    deployment_properties = "@graknlabs_build_tools//:deployment.properties",
#)
