#
# Copyright (C) 2020 Grakn Labs
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

load("@stackb_rules_proto//java:java_grpc_compile.bzl", "java_grpc_compile")

proto_library(
    name = "migrate-proto-lib",
    srcs = [
        "data.proto",
        "migrate.proto",
    ],
)

java_grpc_compile(
    name = "migrate-proto-java-src",
    deps = [":migrate-proto-lib"],
)

java_library(
    name = "migrate-proto-java",
    srcs = [":migrate-proto-java-src"],
    deps = [
        "//dependencies/maven/artifacts/io/grpc:grpc-protobuf",
        "//dependencies/maven/artifacts/io/grpc:grpc-stub",
        "//dependencies/maven/artifacts/io/grpc:grpc-api",
        "//dependencies/maven/artifacts/com/google/guava:guava",
        "//dependencies/maven/artifacts/com/google/protobuf:protobuf-java",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-migrate-proto:{pom_version}"],
)