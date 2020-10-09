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

load("@graknlabs_bazel_distribution//common:rules.bzl", "assemble_targz", "java_deps", "assemble_zip")
load("@graknlabs_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@graknlabs_dependencies//distribution:deployment.bzl", "deployment_private")

package(default_visibility = ["//visibility:private",])

genrule(
    name = "version-src",
    srcs = [
        "templates/Version.java",
        "//:VERSION",
    ],
    cmd = "VERSION=`cat $(location //:VERSION)`;sed -e \"s/{version}/$$VERSION/g\" $(location templates/Version.java) >> $@",
    outs = ["Version.java"],
    visibility = ["//common:__subpackages__"],
)

java_library(
    name = "version",
    srcs = [":version-src"],
    tags = ["maven_coordinates=io.grakn.core:grakn-version:{pom_version}"],
)

java_library(
    name = "server",
    srcs = glob(["*.java", "*/*.java", "*/*/*.java"], exclude=["templates/**", "test/**"]),
    deps = [
        # Internal dependencies
        ":version",
        "//:grakn",
        "//common:common",
        "//concept:concept",
        "//rocks:rocks",

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_grabl_tracing//client:client",
        "@graknlabs_graql//java/pattern:pattern",
        "@graknlabs_graql//java:graql",
        "@graknlabs_graql//java/query",
        "@graknlabs_protocol//grpc/java:protocol",

        # External dependencies from Maven
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:info_picocli_picocli",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:io_netty_netty_all",
        "@maven//:org_slf4j_slf4j_api",
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-server:{pom_version}"],
)

java_binary(
    name = "server-bin",
    main_class = "grakn.core.server.GraknServer",
    runtime_deps = [":server"],
    tags = ["maven_coordinates=io.grakn.core:grakn-server-bin:{pom_version}"],
)

java_deps(
    name = "server-deps",
    target = ":server-bin",
    java_deps_root = "server/lib/common/",
#    java_deps_root_overrides = {
#        "rocksdbjni-dev-*": "server/lib/dev/",
#    },
    java_deps_root_overrides = {
        "rocksdbjni-*": "server/lib/prod/",
    },
    visibility = ["//:__pkg__"],
    maven_name = True,
)

java_deps(
    name = "server-deps-dev",
    target = "@maven//:org_rocksdb_rocksdbjni_dev",
    java_deps_root = "server/lib/dev/",
    visibility = ["//:__pkg__"],
    maven_name = True,
)

# TODO: Once we have a cross-platform rocksdbjni-dev build, make rocksdbjni-dev the default version that we use
#       (by uncommenting server-deps-prod and deleting server-deps-dev)
#java_deps(
#    name = "server-deps-prod",
#    target = "@maven//:org_rocksdb_rocksdbjni",
#    java_deps_root = "server/lib/prod/",
#    visibility = ["//:__pkg__"],
#    maven_name = True,
#)

assemble_deps = [
    ":server-deps",
    ":server-deps-dev",
#    ":server-deps-prod",
    "@graknlabs_common//binary:assemble-bash-targz",
]

assemble_files = {
    "//server/conf:logback.xml": "server/conf/logback.xml",
    "//server/conf:grakn.properties": "server/conf/grakn.properties",
    "//server/resources:logo": "server/resources/grakn-core-ascii.txt",
    "//:LICENSE": "LICENSE",
}

assemble_targz(
    name = "assemble-linux-targz",
    targets = assemble_deps,
    additional_files = assemble_files,
    output_filename = "grakn-core-server-linux",
    visibility = ["//visibility:public"]
)

assemble_zip(
    name = "assemble-mac-zip",
    targets = assemble_deps,
    additional_files = assemble_files,
    output_filename = "grakn-core-server-mac",
)

deploy_artifact(
    name = "deploy-linux-targz",
    target = ":assemble-linux-targz",
    artifact_group = "graknlabs_grakn_core",
    artifact_name = "grakn-core-server-linux-{version}.tar.gz",
    release = deployment_private['artifact.release'],
    snapshot = deployment_private['artifact.snapshot'],
)

deploy_artifact(
    name = "deploy-mac-zip",
    target = ":assemble-mac-zip",
    artifact_group = "graknlabs_grakn_core",
    artifact_name = "grakn-core-server-mac-{version}.tar.gz",
    release = deployment_private['artifact.release'],
    snapshot = deployment_private['artifact.snapshot'],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl",
)
