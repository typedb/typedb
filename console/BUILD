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
load("@graknlabs_bazel_distribution//distribution:rules.bzl", "distribution_structure", "distribution_zip", "distribution_deb", "distribution_rpm")
load("@graknlabs_bazel_distribution//rpm/deployment:rules.bzl", "deploy_rpm")
load("@graknlabs_bazel_distribution//deb/deployment:rules.bzl", "deploy_deb")
load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")


java_library(
    name = "console",
    srcs = glob(["**/*.java"]),
    deps = [
        # Grakn Core dependencies
        "//common:common",
        "//client-java:client-java",
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

deploy_maven_jar(
    name = "deploy-maven-jar",
    target = ":console",
    package = "console",
)

distribution_structure(
    name="grakn-core-console",
    targets = {
        "//console:console-binary": "console/services/lib/"
    },
    additional_files = {
        "//server:conf/logback.xml": "console/conf/logback.xml",
        "//server:conf/grakn.properties": "console/conf/grakn.properties",
    },
    visibility = ["//:__pkg__"]
)

distribution_zip(
    name = "distribution",
    distribution_structures = [":grakn-core-console", "//:grakn-core-bin"],
    additional_files = {
        "//:grakn": 'grakn',
        "//server:conf/logback.xml": "conf/logback.xml",
        "//server:conf/grakn.properties": "conf/grakn.properties",
    },
    output_filename = "grakn-core-console",
)

distribution_deb(
    name = "distribution-deb",
    package_name = "grakn-core-console",
    maintainer = "Grakn Labs <community@grakn.ai>",
    description = "Grakn Core (console)",
    version_file = "//:VERSION",
    depends = [
      "openjdk-8-jre",
      "grakn-core-bin"
    ],
    distribution_structures = [":grakn-core-console"],
    installation_dir = "/opt/grakn/core/",
    empty_dirs = [
         "opt/grakn/core/console/services/lib/",
    ],
)

deploy_deb(
    name = "deploy-deb",
    target = ":distribution-deb",
    deployment_properties = "//:deployment.properties",
)

distribution_rpm(
    name = "distribution-rpm",
    package_name = "grakn-core-console",
    installation_dir = "/opt/grakn/core/",
    version_file = "//:VERSION",
    spec_file = "//dependencies/distribution/rpm:grakn-core-console.spec",
    distribution_structures = [":grakn-core-console"],
    empty_dirs = [
         "opt/grakn/core/console/services/lib/",
    ],
)

deploy_rpm(
    name = "deploy-rpm",
    target = ":distribution-rpm",
    deployment_properties = "//:deployment.properties",
)

test_suite(
    name = "console-test-integration",
    tests = [

    ]
)