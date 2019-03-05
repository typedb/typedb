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
#

load("//dependencies/maven:rules.bzl", "deploy_maven_jar")
load("@graknlabs_build_tools//checkstyle:rules.bzl", "checkstyle_test")

java_library(
    name = "api",
    srcs = glob(["**/*.java"]),
    deps = [
        "//concept:concept",

        # External dependencies from @graknlabs
        "@graknlabs_graql//java:graql",

        # External dependencies from Maven
        "//dependencies/maven/artifacts/com/google/code/findbugs:jsr305",
    ],
    tags = ["maven_coordinates=grakn.core:api:{pom_version}"],
    visibility = ["//visibility:public"]
)


deploy_maven_jar(
    name = "deploy-maven-jar",
    target = ":api",
    package = "api",
)

checkstyle_test(
    name = "checkstyle",
    targets = [
        ":api"
    ],
)