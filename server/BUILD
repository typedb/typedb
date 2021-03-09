#
# Copyright (C) 2021 Grakn Labs
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

load("@graknlabs_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@graknlabs_bazel_distribution//common:rules.bzl", "assemble_targz", "java_deps", "assemble_zip")
load("@graknlabs_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@graknlabs_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@graknlabs_dependencies//distribution:deployment.bzl", "deployment")
load("@graknlabs_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@stackb_rules_proto//java:java_grpc_compile.bzl", "java_grpc_compile")


package(default_visibility = ["//visibility:private",])

genrule(
    name = "version-src",
    srcs = [
        "templates/Version.java",
        "//:VERSION",
    ],
    cmd = "VERSION=`cat $(location //:VERSION)`;sed -e \"s/{version}/$$VERSION/g\" $(location templates/Version.java) >> $@",
    outs = ["Version.java"],
)

java_library(
    name = "version",
    srcs = [":version-src"],
    tags = ["maven_coordinates=io.grakn.core:grakn-version:{pom_version}"],
    visibility = ["//test:__subpackages__"],
)

native_java_libraries(
    name = "server",
    srcs = glob(["*.java", "*/*.java", "*/*/*.java"], exclude=["templates/**", "test/**"]),
    deps = [
        # Internal dependencies
        ":version",
        "//common:common",
        "//concurrent:concurrent",

        # External dependencies from Grakn Labs
        "@graknlabs_common//:common",
        "@graknlabs_grabl_tracing//client:client",
        "@graknlabs_graql//java/pattern:pattern",
        "@graknlabs_graql//java:graql",
        "@graknlabs_graql//java/query",
        "@graknlabs_graql//java/common",
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
    native_libraries_deps = [
        "//:grakn",
        "//concept:concept",
        "//logic:logic",
        "//query:query",
        "//rocks:rocks",
        "//migrator:migrator",
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    tags = ["maven_coordinates=io.grakn.core:grakn-server:{pom_version}"],
    visibility = ["//visibility:public"],
)

java_binary(
    name = "server-bin-mac",
    main_class = "grakn.core.server.GraknServer",
    runtime_deps = [":server-mac"],
    tags = ["maven_coordinates=io.grakn.core:grakn-server-bin:{pom_version}"],
    resources = [ "//server/conf:logback.xml" ],
    data = [ "//server/conf:grakn-properties" ],
    resource_strip_prefix = "server/conf/"
)

java_binary(
    name = "server-bin-linux",
    main_class = "grakn.core.server.GraknServer",
    runtime_deps = [":server-linux"],
    tags = ["maven_coordinates=io.grakn.core:grakn-server-bin:{pom_version}"],
    resources = [ "//server/conf:logback-debug.xml" ],
    data = [ "//server/conf:grakn-properties" ],
    resource_strip_prefix = "server/conf/"
)

java_binary(
    name = "server-bin-windows",
    main_class = "grakn.core.server.GraknServer",
    runtime_deps = [":server-windows"],
    tags = ["maven_coordinates=io.grakn.core:grakn-server-bin:{pom_version}"],
    resources = [ "//server/conf:logback-debug.xml" ],
    data = [ "//server/conf:grakn-properties" ],
    resource_strip_prefix = "server/conf/"
)

java_deps(
    name = "server-deps-mac",
    target = ":server-bin-mac",
    java_deps_root = "server/lib/common/",
    java_deps_root_overrides = {
        "rocksdbjni-dev-*": "server/lib/dev/",
    },
    visibility = ["//:__pkg__"],
    maven_name = True,
)

java_deps(
    name = "server-deps-linux",
    target = ":server-bin-linux",
    java_deps_root = "server/lib/common/",
#    java_deps_root_overrides = {
#        "rocksdbjni-dev-*": "server/lib/dev/",
#    },
    visibility = ["//:__pkg__"],
    maven_name = True,
)

java_deps(
    name = "server-deps-windows",
    target = ":server-bin-windows",
    java_deps_root = "server/lib/common/",
#    java_deps_root_overrides = {
#        "rocksdbjni-dev-*": "server/lib/dev/",
#    },
    visibility = ["//:__pkg__"],
    maven_name = True,
)

java_deps(
    name = "server-deps-prod",
    target = "@maven//:org_rocksdb_rocksdbjni",
    java_deps_root = "server/lib/prod/",
    visibility = ["//:__pkg__"],
    maven_name = True,
)

assemble_deps_common = [
    ":server-deps-prod",
]

assemble_files = {
    "//server/conf:logback": "server/conf/logback-test.xml",
    "//server/conf:logback-debug": "server/conf/logback-debug.xml",
    "//server/conf:grakn-properties": "server/conf/grakn.properties",
    "//server/resources:logo": "server/resources/grakn-core-ascii.txt",
    "//:LICENSE": "LICENSE",
}

permissions = {
    "server/conf/grakn.properties": "0755",
    "server/conf/logback-test.xml": "0755",
    "server/conf/logback-debug.xml": "0755",
}

assemble_targz(
    name = "assemble-linux-targz",
    targets = ["server-deps-linux", "@graknlabs_common//binary:assemble-bash-targz"],
    additional_files = assemble_files,
    permissions = permissions,
    output_filename = "grakn-core-server-linux",
    visibility = ["//:__pkg__", "//test:__subpackages__"]
)

assemble_zip(
    name = "assemble-mac-zip",
    targets = assemble_deps_common + ["server-deps-mac", "@graknlabs_common//binary:assemble-bash-targz"],
    additional_files = assemble_files,
    permissions = permissions,
    output_filename = "grakn-core-server-mac",
    visibility = ["//:__pkg__", "//test:__subpackages__"]
)

assemble_zip(
    name = "assemble-windows-zip",
    targets = ["server-deps-windows", "@graknlabs_common//binary:assemble-bat-targz"],
    additional_files = assemble_files,
    permissions = permissions,
    output_filename = "grakn-core-server-windows",
    visibility = ["//:__pkg__", "//test:__subpackages__"]
)

deploy_artifact(
    name = "deploy-linux-targz",
    target = ":assemble-linux-targz",
    artifact_group = "graknlabs_grakn_core",
    artifact_name = "grakn-core-server-linux-{version}.tar.gz",
    release = deployment['artifact.release'],
    snapshot = deployment['artifact.snapshot'],
)

deploy_artifact(
    name = "deploy-mac-zip",
    target = ":assemble-mac-zip",
    artifact_group = "graknlabs_grakn_core",
    artifact_name = "grakn-core-server-mac-{version}.zip",
    release = deployment['artifact.release'],
    snapshot = deployment['artifact.snapshot'],
)

deploy_artifact(
    name = "deploy-windows-zip",
    target = ":assemble-windows-zip",
    artifact_group = "graknlabs_grakn_core",
    artifact_name = "grakn-core-server-windows-{version}.zip",
    release = deployment['artifact.release'],
    snapshot = deployment['artifact.snapshot'],
)

assemble_apt(
    name = "assemble-linux-apt",
    package_name = "grakn-core-server",
    maintainer = "Grakn Labs <community@grakn.ai>",
    description = "Grakn Core (server)",
    depends = [
      "openjdk-11-jre",
      "grakn-bin (=%{@graknlabs_common})"
    ],
    workspace_refs = "@graknlabs_grakn_core_workspace_refs//:refs.json",
    archives = [":server-deps-linux"],
    installation_dir = "/opt/grakn/core/",
    files = assemble_files,
    empty_dirs = [
        "opt/grakn/core/server/lib/dev",
        "opt/grakn/core/server/lib/prod",
        "opt/grakn/core/server/lib/common",
        "var/lib/grakn/db/"
     ],
    permissions = {
        "var/lib/grakn/db/": "0777",
    },
    symlinks = {
        "opt/grakn/core/server/data": "/var/lib/grakn/db/",
    },
)

deploy_apt(
    name = "deploy-apt",
    target = ":assemble-linux-apt",
    snapshot = deployment['apt.snapshot'],
    release = deployment['apt.release']
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl",
)

