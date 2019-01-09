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
load("//dependencies/maven:rules.bzl", "deploy_maven_jar")
load("@graknlabs_rules_deployment//distribution:rules.bzl", "distribution", "deploy_deb", "deploy_rpm")

java_library(
    name = "console",
    srcs = glob(["**/*.java"]),
    deps = [
        # Grakn Core dependencies
        "//client-java",
        "//server", # NEEDS TO GO
        "//common",

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

load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")
checkstyle_test(
 name = "console-checkstyle",
 target = ":console",
 config = "//config/checkstyle:checkstyle.xml",
 suppressions = "//config/checkstyle:checkstyle-suppressions.xml",
 licenses = ["//config/checkstyle:licenses"],
)

java_binary(
    name = "console-binary",
    main_class = "grakn.core.console.GraknConsole",
    runtime_deps = [":console"],
    visibility = ["//:__pkg__"],
)

deploy_maven_jar(
    name = "deploy-maven-jar",
    target = ":console",
    package = "console",
)

distribution(
    name = "distribution",
    targets = {
        "//console:console-binary": "console/services/lib/"
    },
    additional_files = {
        "//:grakn": 'grakn',
        "//server:conf/logback.xml": "conf/logback.xml",
        "//server:conf/grakn.properties": "conf/grakn.properties",
    },
    output_filename = "grakn-core-console",
)

deploy_deb(
    name = "deploy-deb",
    package_name = "grakn-core-console",
    maintainer = "Grakn Labs <community@grakn.ai>",
    description = "Grakn Core (console)",
    version_file = "//:VERSION",
    depends = [
      "openjdk-8-jre",
      "grakn-core-bin"
    ],
    target = ":console-binary",
    installation_dir = "/opt/grakn/core/console/",
    empty_dirs = [
         "opt/grakn/core/console/services/lib/",
    ],
)


deploy_rpm(
    name = "deploy-rpm",
    package_name = "grakn-core-console",
    installation_dir = "/opt/grakn/core/console/",
    version_file = "//:VERSION",
    spec_file = "//dependencies/distribution/rpm:grakn-core-console.spec",
    target = ":console-binary",
    empty_dirs = [
         "opt/grakn/core/console/services/lib/",
    ],
)

test_suite(
    name = "console-test-integration",
    tests = [

    ]
)