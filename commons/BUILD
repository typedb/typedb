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
load("//dependencies/deployment/maven:rules.bzl", "deploy_maven_jar")

java_library(
    name = "commons",
    srcs = glob(
        ["**/*.java"],
        exclude = ["**/test/*.java"]
    ),
    deps = [
        "//dependencies/maven/artifacts/com/google/auto/value:auto-value",
        "//dependencies/maven/artifacts/com/google/code/findbugs:annotations",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/ch/qos/logback:logback-core",
    ],
    tags = ["maven_coordinates=grakn.core:commons:{pom_version}"],
)


deploy_maven_jar(
    name = "deploy-maven-jar",
    targets = [":commons"],
    version_file = "//:VERSION",
)

java_test(
    name = "grakn-config-test",
    test_class = "grakn.core.commons.config.test.ConfigTest",
    srcs = ["config/test/ConfigTest.java"],
    deps = [":commons"],
    resources = ["//server:conf/grakn.properties"],
    size = "small"
)