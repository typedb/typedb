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

load("@org_pubref_rules_proto//java:compile.bzl", "java_grpc_compile")
load("//dependencies/deployment/maven:rules.bzl", "deploy_maven_jar")

java_grpc_compile(
    name = "protocol-java-src",
    deps = [
        "//protocol/session:session-proto",
        "//protocol/session:answer-proto",
        "//protocol/session:concept-proto",
        "//protocol/keyspace:keyspace-proto",
    ]
)

java_library(
    name = "protocol-java",
    srcs = [":protocol-java-src"],
    deps = [
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
        "//dependencies/maven/artifacts/io/grpc:grpc-core",
        "//dependencies/maven/artifacts/io/grpc:grpc-protobuf",
        "//dependencies/maven/artifacts/io/grpc:grpc-stub",
    ],
    tags = ["maven_coordinates=grakn.core:protocol:{pom_version}"],
)

deploy_maven_jar(
    name = "deploy-maven-jar",
    targets = [":protocol-java"],
    version_file = "//:VERSION",
)


load("@org_pubref_rules_proto//python:compile.bzl", "python_grpc_compile")
load("@io_bazel_rules_python//python:python.bzl", "py_library")
load("@pypi_dependencies//:requirements.bzl", "requirement")

python_grpc_compile(
    name = "protocol_python_src",
    deps = [
        "//protocol/session:session-proto",
        "//protocol/session:answer-proto",
        "//protocol/session:concept-proto",
        "//protocol/keyspace:keyspace-proto",
    ]
)

py_library(
    name = "protocol_python",
    srcs = [":protocol_python_src"],
    deps = [
        requirement("protobuf"),
        requirement("grpcio"),
        requirement("six"),
        requirement("enum34")
    ]
)