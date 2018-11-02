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

load("//dependencies/deployment/maven:rules.bzl", "deploy_maven_jar")

java_library(
    name = "console",
    srcs = glob(["src/**/*.java"]),
    deps = [
        # Grakn Core dependencies
        "//client-java:client-java",
        "//grakn-graql:grakn-graql",

        # External dependencies
        "//dependencies/maven/artifacts/com/google/code/findbugs:annotations",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/commons-cli",
        "//dependencies/maven/artifacts/commons-lang:commons-lang", # PREVOIUSLY UNDECLARED
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/jline:jline",
        "//dependencies/maven/artifacts/org/hamcrest:hamcrest-all",
        "//dependencies/maven/artifacts/org/mockito:mockito-core",
        "//dependencies/maven/artifacts/ch/qos/logback:logback-classic",
        "//dependencies/maven/artifacts/org/slf4j:slf4j-api",
    ],
    visibility = ["//visibility:public"],
    runtime_deps = [
        #This needs to be available in the classpath otherwise Logback will print error messages:
        "//dependencies/maven/artifacts/org/codehaus/janino:janino"
    ],
    tags = ["maven_coordinates=ai.grakn:core.console:{pom_version}"],
)

deploy_maven_jar(
    name = "deploy-maven-jar",
    targets = [":console"],
    version_file = "//:VERSION",
)

java_binary(
    name = "console-binary",
    main_class = "ai.grakn.core.console.Graql",
    runtime_deps = ["//console:console"],
    visibility = ["//visibility:public"]
)

genrule(
    name = "distribution",
    srcs = ["//:grakn", "console-binary_deploy.jar"],
    outs = ["dist/grakn-core-console.zip"],
    cmd  = "$(location distribution.sh) $(location dist/grakn-core-console.zip) $(location //:grakn) $(location console-binary_deploy.jar)",
    tools = ["distribution.sh"]
)

exports_files(
    ["conf/logback.xml"],
    visibility = ["//visibility:public"]
)