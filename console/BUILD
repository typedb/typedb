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

package(default_visibility = ["//visibility:__subpackages__"])
load("//dependencies/maven:rules.bzl", "assemble_maven", "deploy_maven")
load("@graknlabs_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@graknlabs_bazel_distribution//common:rules.bzl", "assemble_targz", "java_deps", "assemble_zip")
load("@graknlabs_bazel_distribution//rpm:rules.bzl", "assemble_rpm", "deploy_rpm")
load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")


java_library(
    name = "console",
    srcs = glob(["**/*.java"]),
    deps = [
        # Grakn Core dependencies
        "//common:common",
        "@graknlabs_client_java//:client-java",
        "@graknlabs_graql//java:graql",
        "//server:server", # NEEDS TO GO

        # External dependencies
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/ch/qos/logback:logback-core",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/commons-cli",
        "//dependencies/maven/artifacts/commons-lang:commons-lang", # PREVOIUSLY UNDECLARED
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/jline:jline",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/org/codehaus/janino:janino", # Needed to avoid Logback error
    ],
    visibility = ["//console/test:__subpackages__"],
    resources = ["LICENSE"],
    resource_strip_prefix = "console",
    tags = ["maven_coordinates=grakn.core:console:{pom_version}"],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":console"
    ],
)

java_binary(
    name = "console-binary",
    main_class = "grakn.core.console.GraknConsole",
    runtime_deps = [":console"],
    visibility = ["//:__pkg__"],
)

java_deps(
    name = "console-deps",
    target = ":console-binary",
    java_deps_root = "console/services/lib/",
    version_file = "//:VERSION",
    visibility = ["//:__pkg__"]
)

assemble_targz(
    name = "assemble-linux-targz",
    output_filename = "grakn-core-console",
    targets = [":console-deps", "//bin:assemble-bash-targz"],
    empty_directories = [
        "console/db/cassandra",
        "console/db/queue"
    ],
    additional_files = {
        "//server:conf/logback.xml": "console/conf/logback.xml",
        "//server:conf/grakn.properties": "console/conf/grakn.properties",
    },
    permissions = {
      "console/services/cassandra/cassandra.yaml": "0777",
      "console/db/cassandra": "0777",
      "console/db/queue": "0777",
    }
)

assemble_zip(
    name = "assemble-mac-zip",
    output_filename = "grakn-core-console-mac",
    targets = [":console-deps", "//bin:assemble-bash-targz"],
    empty_directories = [
        "console/db/cassandra",
        "console/db/queue"
    ],
    additional_files = {
        "//server:conf/logback.xml": "console/conf/logback.xml",
        "//server:conf/grakn.properties": "console/conf/grakn.properties",
    },
    permissions = {
      "console/services/cassandra/cassandra.yaml": "0777",
      "console/db/cassandra": "0777",
      "console/db/queue": "0777",
    }
)

assemble_zip(
    name = "assemble-windows-zip",
    output_filename = "grakn-core-console-windows",
    targets = [":console-deps", "//bin:assemble-bat-targz"],
    empty_directories = [
        "console/db/cassandra",
        "console/db/queue"
    ],
    additional_files = {
        "//server:conf/logback.xml": "console/conf/logback.xml",
        "//server:conf/grakn.properties": "console/conf/grakn.properties",
    },
    permissions = {
      "console/services/cassandra/cassandra.yaml": "0777",
      "console/db/cassandra": "0777",
      "console/db/queue": "0777",
    }
)

assemble_maven(
    name = "assemble-maven",
    target = ":console",
    package = "console",
)

deploy_maven(
    name = "deploy-maven",
    target = ":assemble-maven"
)

assemble_apt(
    name = "assemble-linux-apt",
    package_name = "grakn-core-console",
    maintainer = "Grakn Labs <community@grakn.ai>",
    description = "Grakn Core (console)",
    version_file = "//:VERSION",
    depends = [
      "openjdk-8-jre",
      "grakn-core-bin"
    ],
    files = {
        "//server:conf/logback.xml": "console/conf/logback.xml",
        "//server:conf/grakn.properties": "console/conf/grakn.properties",
    },
    archives = [":console-deps"],
    installation_dir = "/opt/grakn/core/",
    empty_dirs = [
         "opt/grakn/core/console/services/lib/",
    ],
)

deploy_apt(
    name = "deploy-apt",
    target = ":assemble-linux-apt",
    deployment_properties = "@graknlabs_build_tools//:deployment.properties",
)

assemble_rpm(
    name = "assemble-linux-rpm",
    package_name = "grakn-core-console",
    installation_dir = "/opt/grakn/core/",
    version_file = "//:VERSION",
    spec_file = "//config/rpm:grakn-core-console.spec",
    archives = [":console-deps"],
    files = {
        "//server:conf/logback.xml": "console/conf/logback.xml",
        "//server:conf/grakn.properties": "console/conf/grakn.properties",
    },
    empty_dirs = [
         "opt/grakn/core/console/services/lib/",
    ],
)

deploy_rpm(
    name = "deploy-rpm",
    target = ":assemble-linux-rpm",
    deployment_properties = "@graknlabs_build_tools//:deployment.properties",
)

test_suite(
    name = "console-test-integration",
    tests = []
)