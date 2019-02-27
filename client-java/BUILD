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

load("//dependencies/maven:rules.bzl", "deploy_maven_jar")
load("//dependencies/tools/checkstyle:checkstyle.bzl", "checkstyle_test")

java_library(
    name = "client-java",
    srcs = glob(["**/*.java"], exclude = ["**/test/*.java"]),
    deps = [
        # Internal dependencies
        "//api:api",
        "//common:common",
        "//concept:concept",
        "//protocol:protocol-java",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-stub",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api"
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/ch/qos/logback:logback-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-netty",
        "//dependencies/maven/artifacts/io/netty:netty-all",
    ],
    resources = ["LICENSE"],
    tags = ["maven_coordinates=grakn.core:client:{pom_version}"],
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":client-java"
    ],
)

deploy_maven_jar(
    name = "deploy-maven-jar",
    target = ":client-java",
    package = "client-java",
)
