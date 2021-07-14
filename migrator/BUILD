#
# Copyright (C) 2021 Vaticle
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

load("@vaticle_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@vaticle_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_zip", "java_deps")
load("@vaticle_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@stackb_rules_proto//java:java_grpc_compile.bzl", "java_grpc_compile")

package(default_visibility = ["//visibility:private",])

proto_library(
    name = "migrator-proto",
    srcs = ["migrator.proto"],
)

proto_library(
    name = "migrator-data",
    srcs = ["data/data.proto"],
)

java_grpc_compile(
    name = "migrator-grpc-src",
    deps = ["migrator-proto"],
)

java_grpc_compile(
    name = "migrator-data-grpc-src",
    deps = ["migrator-data"],
)

native_java_libraries(
    name = "migrator",
    srcs = [":migrator-grpc-src", ":migrator-data-grpc-src"] + glob(["*.java", "*/*.java", "*/*/*.java"]),
    deps = [
        # Internal dependencies
        "//common:common",
        "//concurrent",

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_typeql_lang_java//pattern:pattern",
        "@vaticle_typeql_lang_java//common:common",

        # External dependencies from Maven
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:javax_annotation_javax_annotation_api", # gRPC needs this in order to compile in Java 11 and Java 14
        "@maven//:org_slf4j_slf4j_api",
    ],
    native_libraries_deps = [
        "//:typedb",
        "//concept:concept",
        "//logic:logic",
        "//rocks:rocks",
    ],
    mac_deps = [
        "@maven//:org_rocksdb_rocksdbjni_dev"
    ],
    linux_deps = [
        "@maven//:org_rocksdb_rocksdbjni",
    ],
    windows_deps = [
        "@maven//:org_rocksdb_rocksdbjni",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-migrator:{pom_version}"],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl",
)

