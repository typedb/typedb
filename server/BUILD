#
# Copyright (C) 2020 Grakn Labs
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
load("@graknlabs_bazel_distribution//common:rules.bzl", "assemble_targz", "java_deps", "assemble_zip")
load("@graknlabs_bazel_distribution//rpm:rules.bzl", "assemble_rpm", "deploy_rpm")
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

exports_files(
    glob(["conf/**", "services/**"]),
    visibility = ["//visibility:public"]
)


genrule(
    name = "version",
    srcs = [
        "templates/Version.java",
        "//:VERSION",
    ],
    cmd = "VERSION=`cat $(location //:VERSION)`;sed -e \"s/{version}/$$VERSION/g\" $(location templates/Version.java) >> $@",
    outs = ["Version.java"],
    visibility = ["//common:__subpackages__"],
)

java_library(
    name = "server",
    srcs = glob(["**/*.java"], exclude=["templates/**", "test/**"]) + [":version"],
    deps = [
        # Internal dependencies
        "//common",
        "//kb/concept/api",
        "//kb/concept/structure",
        "//kb/concept/manager",
        "//concept/structure",
        "//concept/impl",
        "//concept/answer",
        "//concept/util",
        "//concept/manager",
        "//kb/server",
        "//kb/keyspace",
        "//kb/graql/reasoner",
        "//kb/graql/executor",
        "//kb/graql/planning",
        "//kb/graql/exception",
        "//keyspace",
        "//core",
        "//graql/executor",
        "//graql/planning",
        "//graql/reasoner",
        "//graph",


        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",
        "@graknlabs_common//:common",
        "@graknlabs_protocol//grpc/java:protocol",
        "@graknlabs_grabl_tracing//client",

        # External dependencies from Maven
        "@maven//:org_apache_hadoop_hadoop-annotations",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_guava_guava",
        "@maven//:commons_cli_commons_cli",
        "@maven//:commons_configuration_commons_configuration",
        "@maven//:commons_lang_commons_lang",
        "@maven//:io_grpc_grpc-core",
        "@maven//:io_grpc_grpc-netty",
        "@maven//:io_grpc_grpc-stub",
        "@maven//:io_grpc_grpc-api",
        "@maven//:io_netty_netty-all",
        "@maven//:org_apache_cassandra_cassandra-all",
        "@maven//:org_apache_commons_commons-lang3",
        "@maven//:org_apache_hadoop_hadoop-common",
        "@maven//:org_apache_hadoop_hadoop-mapreduce-client-core",
        "@maven//:org_apache_spark_spark-core-2-11",
        "@maven//:org_apache_spark_spark-launcher-2-11",
        "@maven//:org_apache_tinkerpop_gremlin-core",
        "@maven//:org_apache_tinkerpop_hadoop-gremlin",
        "@maven//:org_apache_tinkerpop_spark-gremlin",
        "@maven//:org_apache_tinkerpop_tinkergraph-gremlin",
        "@maven//:org_scala-lang_scala-library",
        "@maven//:org_slf4j_slf4j-api",
        "@maven//:com_datastax_oss_java-driver-core"
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback-classic",
        "@maven//:ch_qos_logback_logback-core",
        "@maven//:javax_servlet_javax-servlet-api",
        "//mapreduce:mapreduce"
    ],
    resources = ["LICENSE"] + glob(["resources/*"]),
    resource_strip_prefix = "server",
    tags = ["maven_coordinates=io.grakn.core:grakn-server:{pom_version}"],
    visibility = ["//visibility:public"]

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
        "//server:server"
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-server-bin:{pom_version}"],
    visibility = ["//:__pkg__"],
)

java_binary(
    name = "server-core-binary",
    main_class = "grakn.core.server.Grakn",
    runtime_deps = [
        "//server:server"
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-server-bin:{pom_version}"],
    visibility = ["//:__pkg__"],
)

java_binary(
    name = "server-storage-binary",
    main_class = "grakn.core.server.GraknStorage",
    runtime_deps = [
        "//server:server"
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-server-bin:{pom_version}"],
    visibility = ["//:__pkg__"],
)

java_deps(
    name = "server-deps",
    target = ":server-binary",
    java_deps_root = "server/services/lib/",
    visibility = ["//:__pkg__"],
    maven_name = True,
)

assemble_targz(
    name = "assemble-linux-targz",
    output_filename = "grakn-core-server-linux",
    targets = [":server-deps", "@graknlabs_dependencies//distribution:assemble-bash-targz"],
    additional_files = {
        "//server:conf/logback.xml": "server/conf/logback.xml",
        "//server:conf/grakn.properties": "server/conf/grakn.properties",
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
    },
    visibility = ["//visibility:public"]
)

assemble_zip(
    name = "assemble-mac-zip",
    output_filename = "grakn-core-server-mac",
    targets = [":server-deps", "@graknlabs_dependencies//distribution:assemble-bash-targz"],
    additional_files = {
        "//server:conf/logback.xml": "server/conf/logback.xml",
        "//server:conf/grakn.properties": "server/conf/grakn.properties",
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
    },
    visibility = ["//visibility:public"]
)

assemble_zip(
    name = "assemble-windows-zip",
    output_filename = "grakn-core-server-windows",
    targets = [":server-deps", "@graknlabs_dependencies//distribution:assemble-bat-targz"],
    additional_files = {
        "//server:conf/logback.xml": "server/conf/logback.xml",
        "//server:conf/grakn.properties": "server/conf/grakn.properties",
        "//server:services/cassandra/cassandra.yaml": "server/services/cassandra/cassandra.yaml",
        "//server:services/cassandra/logback.xml": "server/services/cassandra/logback.xml",
        "//server:services/grakn/grakn-core-ascii.txt": "server/services/grakn/grakn-core-ascii.txt",
        "//server:services/hadoop/bin/winutils.exe": "server/services/hadoop/bin/winutils.exe"
    },
    empty_directories = [
        "server/db/cassandra",
        "server/db/queue"
    ],
    permissions = {
      "server/services/cassandra/cassandra.yaml": "0777",
      "server/db/cassandra": "0777",
      "server/db/queue": "0777",
    },
    visibility = ["//visibility:public"]
)

assemble_apt(
    name = "assemble-linux-apt",
    package_name = "grakn-core-server",
    maintainer = "Grakn Labs <community@grakn.ai>",
    description = "Grakn Core (server)",
    depends = [
      "openjdk-8-jre",
      "grakn-bin (>=%{@graknlabs_common})"
    ],
    workspace_refs = "@graknlabs_grakn_core_workspace_refs//:refs.json",
    archives = [":server-deps"],
    installation_dir = "/opt/grakn/core/",
    files = {
        "//server:conf/logback.xml": "server/conf/logback.xml",
        "//server:conf/grakn.properties": "server/conf/grakn.properties",
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
    target = ":assemble-linux-apt",
    deployment_properties = "@graknlabs_dependencies//distribution:deployment.properties",
)

assemble_rpm(
    name = "assemble-linux-rpm",
    package_name = "grakn-core-server",
    installation_dir = "/opt/grakn/core/",
    spec_file = "//config/rpm:grakn-core-server.spec",
    workspace_refs = "@graknlabs_grakn_core_workspace_refs//:refs.json",
    archives = [":server-deps"],
    empty_dirs = [
        "opt/grakn/core/server/services/lib/",
        "var/lib/grakn/db/queue",
        "var/lib/grakn/db/cassandra"
     ],
    files = {
        "//server:conf/logback.xml": "server/conf/logback.xml",
        "//server:conf/grakn.properties": "server/conf/grakn.properties",
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

deploy_rpm(
    name = "deploy-rpm",
    target = ":assemble-linux-rpm",
    deployment_properties = "@graknlabs_dependencies//distribution:deployment.properties",
)
