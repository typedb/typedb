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

java_library(
    name = "common",
    srcs = ["//common/templates/util:grakn-version"],
    exports = [
        "//common/exception",
        "//common/config",
        "//common/http",
        "//common/util",
    ],
    runtime_deps = [
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/ch/qos/logback:logback-core",
    ],
    tags = ["maven_coordinates=grakn.core:common:{pom_version}"],
)


deploy_maven_jar(
    name = "deploy-maven-jar",
    target = ":common",
    package = "common",
    deps = [
        "//common/exception",
        "//common/config",
        "//common/http",
        "//common/util"
    ],
    version_file = "//:VERSION",
    deployment_properties = "//:deployment.properties",
)
