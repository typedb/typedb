# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_zip", "java_deps")
load("@vaticle_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@rules_proto_grpc//java:defs.bzl", "java_grpc_library")

package(default_visibility = ["//visibility:private",])

proto_library(
    name = "migrator-proto",
    srcs = [":migrator.proto"],
)

proto_library(
    name = "database-proto",
    srcs = [":database/data.proto"],
)

java_grpc_library(
    name = "migrator-protocol",
    protos = [":migrator-proto"],
    visibility = ["//visibility:public"],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-migrator-protocol:{pom_version}"],
)

java_grpc_library(
    name = "migrator-data",
    protos = [":database-proto"],
    visibility = ["//visibility:public"],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-migrator-data:{pom_version}"],
)

native_java_libraries(
    name = "migrator",
    srcs = glob(["*.java", "*/*.java", "*/*/*.java"]),
    native_libraries_deps = [
        # Internal dependencies
        "//:typedb",
        "//common:common",
        "//concept:concept",
        "//concurrent:concurrent",
        "//encoding:encoding",
        "//logic:logic",
    ],
    deps = [
        ":migrator-protocol",
        ":migrator-data",

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/pattern:pattern",
        "@vaticle_typeql//java/common:common",

        # External dependencies from Maven
        "@maven//:com_google_guava_guava",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_protobuf",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:io_github_speedb_io_speedbjni",
        "@maven//:io_sentry_sentry",
        "@maven//:javax_annotation_javax_annotation_api", # gRPC needs this in order to compile in Java 11 and Java 14
        "@maven//:org_slf4j_slf4j_api",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-migrator:{pom_version}"],
    visibility = ["//visibility:public"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "mpl-header",
)
