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
load("@rules_deployment//maven:rules.bzl", "deploy_maven_jar")

java_library(
    name = "common",
    srcs = [":grakn-version"] + glob(
        ["**/*.java"],
        exclude = [
            "**/test/*.java",
            "templates/**",
        ]
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
    tags = ["maven_coordinates=grakn.core:common:{pom_version}"],
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

deploy_maven_jar(
    name = "deploy-maven-jar",
    targets = [":common"],
    version_file = "//:VERSION",
    deployment_properties = "//:deployment.properties",
)

java_test(
    name = "grakn-config-test",
    test_class = "grakn.core.common.config.test.ConfigTest",
    srcs = ["config/test/ConfigTest.java"],
    deps = [":common"],
    resources = ["//server:conf/grakn.properties"],
    size = "small"
)