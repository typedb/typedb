#
# GRAKN.AI - THE KNOWLEDGE GRAPH
# Copyright (C) 2019 Grakn Labs Ltd
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

load("@graknlabs_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@graknlabs_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_zip", "java_deps")
load("@graknlabs_bazel_distribution//rpm:rules.bzl", "assemble_rpm", "deploy_rpm")
load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

exports_files(
    glob([
        "conf/**",
        "services/**",
    ]),
    visibility = ["//visibility:public"],
)

genrule(
    name = "version",
    srcs = [
        "templates/Version.java",
        "//:VERSION",
    ],
    outs = ["Version.java"],
    cmd = "VERSION=`cat $(location //:VERSION)`;sed -e \"s/{version}/$$VERSION/g\" $(location templates/Version.java) >> $@",
    visibility = ["//common:__subpackages__"],
)

java_library(
    name = "server",
    srcs = glob(
        ["**/*.java"],
        exclude = [
            "templates/**",
            "test/**",
        ],
    ) + [":version"],
    resource_strip_prefix = "server",
    resources = ["LICENSE"] + glob(["resources/*"]),
    tags = ["maven_coordinates=io.grakn.core:grakn-server:{pom_version}"],
    visibility = ["//visibility:public"],
    runtime_deps = [
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/ch/qos/logback:logback-core",
        "//dependencies/maven/artifacts/javax/servlet:javax-servlet-api",
        "//graph-cql",
        "//graph-hadoop",
    ],
    deps = [
        # Internal dependencies
        "//common",
        "@graknlabs_common//:common",
        "//kb/concept/api",
        "//kb/concept/util",
        "//kb/concept/structure",
        "//concept/structure:structureimpl",
        "//concept/impl",
        "//concept/answer",
        "//kb",
        "//core",
        "//graql",
        "//graph",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",
        "@graknlabs_protocol//grpc/java:protocol",
        "@graknlabs_benchmark//lib:server-instrumentation",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop-annotations",
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/google/guava",
        "//dependencies/maven/artifacts/commons-cli",
        "//dependencies/maven/artifacts/commons-configuration",
        "//dependencies/maven/artifacts/commons-lang",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-netty",
        "//dependencies/maven/artifacts/io/grpc:grpc-stub",
        "//dependencies/maven/artifacts/io/netty:netty-all",
        "//dependencies/maven/artifacts/io/zipkin/brave",
        "//dependencies/maven/artifacts/org/apache/cassandra:cassandra-all",
        "//dependencies/maven/artifacts/org/apache/commons:commons-lang3",
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop-common",
        "//dependencies/maven/artifacts/org/apache/hadoop:hadoop-mapreduce-client-core",
        "//dependencies/maven/artifacts/org/apache/spark:spark-core-2-11",
        "//dependencies/maven/artifacts/org/apache/spark:spark-launcher-2-11",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:gremlin-core",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:hadoop-gremlin",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:spark-gremlin",
        "//dependencies/maven/artifacts/org/apache/tinkerpop:tinkergraph-gremlin",
        "//dependencies/maven/artifacts/org/scala-lang:scala-library",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
        "//dependencies/maven/artifacts/com/datastax/oss:java-driver-core",
    ],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":server",
    ],
)

java_binary(
    name = "server-binary",
    main_class = "grakn.core.daemon.GraknDaemon",
    tags = ["maven_coordinates=io.grakn.core:grakn-server-bin:{pom_version}"],
    visibility = ["//:__pkg__"],
    runtime_deps = [
        "//daemon",
        "//server",
    ],
)

java_deps(
    name = "server-deps",
    java_deps_root = "server/services/lib/",
    maven_name = True,
    target = ":server-binary",
    visibility = ["//:__pkg__"],
)

assemble_targz(
    name = "assemble-linux-targz",
    additional_files = {
        "//server:conf/logback.xml": "server/conf/logback.xml",
        "//server:conf/grakn.properties": "server/conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt",
    },
    empty_directories = [
        "server/db/cassandra",
        "server/db/queue",
    ],
    output_filename = "grakn-core-server-linux",
    permissions = {
        "server/services/cassandra/cassandra.yaml": "0777",
        "server/db/cassandra": "0777",
        "server/db/queue": "0777",
    },
    targets = [
        ":server-deps",
        "@graknlabs_common//bin:assemble-bash-targz",
    ],
    visibility = ["//visibility:public"],
)

assemble_zip(
    name = "assemble-mac-zip",
    additional_files = {
        "//server:conf/logback.xml": "server/conf/logback.xml",
        "//server:conf/grakn.properties": "server/conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt",
    },
    empty_directories = [
        "server/db/cassandra",
        "server/db/queue",
    ],
    output_filename = "grakn-core-server-mac",
    permissions = {
        "server/services/cassandra/cassandra.yaml": "0777",
        "server/db/cassandra": "0777",
        "server/db/queue": "0777",
    },
    targets = [
        ":server-deps",
        "@graknlabs_common//bin:assemble-bash-targz",
    ],
    visibility = ["//visibility:public"],
)

assemble_zip(
    name = "assemble-windows-zip",
    additional_files = {
        "//server:conf/logback.xml": "server/conf/logback.xml",
        "//server:conf/grakn.properties": "server/conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt",
        "//server:services/hadoop/bin/winutils.exe": "server/services/hadoop/bin/winutils.exe",
    },
    empty_directories = [
        "server/db/cassandra",
        "server/db/queue",
    ],
    output_filename = "grakn-core-server-windows",
    permissions = {
        "server/services/cassandra/cassandra.yaml": "0777",
        "server/db/cassandra": "0777",
        "server/db/queue": "0777",
    },
    targets = [
        ":server-deps",
        "@graknlabs_common//bin:assemble-bat-targz",
    ],
    visibility = ["//visibility:public"],
)

assemble_apt(
    name = "assemble-linux-apt",
    package_name = "grakn-core-server",
    archives = [":server-deps"],
    depends = [
        "openjdk-8-jre",
        "grakn-bin (>=%{@graknlabs_common})",
    ],
    description = "Grakn Core (server)",
    empty_dirs = [
        "opt/grakn/core/server/services/lib/",
        "var/lib/grakn/db/queue",
        "var/lib/grakn/db/cassandra",
    ],
    files = {
        "//server:conf/logback.xml": "server/conf/logback.xml",
        "//server:conf/grakn.properties": "server/conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt",
    },
    installation_dir = "/opt/grakn/core/",
    maintainer = "Grakn Labs <community@grakn.ai>",
    permissions = {
        "var/lib/grakn/db/queue": "0777",
        "var/lib/grakn/db/cassandra": "0777",
    },
    symlinks = {
        "opt/grakn/core/server/db/": "/var/lib/grakn/db/",
    },
    workspace_refs = "@graknlabs_grakn_core_workspace_refs//:refs.json",
)

deploy_apt(
    name = "deploy-apt",
    deployment_properties = "@graknlabs_build_tools//:deployment.properties",
    target = ":assemble-linux-apt",
)

assemble_rpm(
    name = "assemble-linux-rpm",
    package_name = "grakn-core-server",
    archives = [":server-deps"],
    empty_dirs = [
        "opt/grakn/core/server/services/lib/",
        "var/lib/grakn/db/queue",
        "var/lib/grakn/db/cassandra",
    ],
    files = {
        "//server:conf/logback.xml": "server/conf/logback.xml",
        "//server:conf/grakn.properties": "server/conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt",
    },
    installation_dir = "/opt/grakn/core/",
    permissions = {
        "services/cassandra/cassandra.yaml": "0777",
        "var/lib/grakn/db/queue": "0777",
        "var/lib/grakn/db/cassandra": "0777",
    },
    spec_file = "//config/rpm:grakn-core-server.spec",
    symlinks = {
        "opt/grakn/core/server/db/": "/var/lib/grakn/db/",
    },
    workspace_refs = "@graknlabs_grakn_core_workspace_refs//:refs.json",
)

deploy_rpm(
    name = "deploy-rpm",
    deployment_properties = "@graknlabs_build_tools//:deployment.properties",
    target = ":assemble-linux-rpm",
)
