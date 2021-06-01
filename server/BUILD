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
load("@vaticle_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@vaticle_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_zip", "java_deps")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
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
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-version:{pom_version}"],
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

        # External dependencies from Vaticle
        "@vaticle_typedb_common//:common",
        "@vaticle_factory_tracing//client:client",
        "@vaticle_typeql_lang_java//pattern:pattern",
        "@vaticle_typeql_lang_java//:typeql-lang",
        "@vaticle_typeql_lang_java//query",
        "@vaticle_typeql_lang_java//common",
        "@vaticle_typedb_protocol//grpc/java:typedb-protocol",

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
        "//:typedb",
        "//concept:concept",
        "//logic:logic",
        "//query:query",
        "//rocks:rocks",
        "//reasoner:reasoner",
        "//migrator:migrator",
    ],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-server:{pom_version}"],
    visibility = ["//visibility:public"],
)

java_binary(
    name = "server-bin-mac",
    main_class = "com.vaticle.typedb.core.server.TypeDBServer",
    runtime_deps = [":server-mac"],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-server-bin:{pom_version}"],
    classpath_resources = [
        "//server/conf:logback.xml",
        "//server/conf:typedb-properties"
    ],
    data = [ "//server/conf:typedb-properties" ], # Separately needed for com.vaticle.typedb.core.server.common.Util
)

java_binary( # TODO: this target should not be needed if :server-deps-mac can depend directly on :server-mac (lib)
    name = "server-bin-deps-mac",
    main_class = "com.vaticle.typedb.core.server.TypeDBServer",
    runtime_deps = [":server-mac"],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-server-bin-deps:{pom_version}"],
)

java_binary(
    name = "server-bin-linux",
    main_class = "com.vaticle.typedb.core.server.TypeDBServer",
    runtime_deps = [":server-linux"],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-server-bin:{pom_version}"],
    classpath_resources = [
        "//server/conf:logback.xml",
        "//server/conf:typedb-properties"
    ],
    data = [ "//server/conf:typedb-properties" ], # Separately needed for com.vaticle.typedb.core.server.common.Util
)

java_binary( # TODO: this target should not be needed if :server-deps-linux can depend directly on :server-linux (lib)
    name = "server-bin-deps-linux",
    main_class = "com.vaticle.typedb.core.server.TypeDBServer",
    runtime_deps = [":server-linux"],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-server-bin-deps:{pom_version}"],
)

java_binary(
    name = "server-bin-windows",
    main_class = "com.vaticle.typedb.core.server.TypeDBServer",
    runtime_deps = [":server-windows"],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-server-bin:{pom_version}"],
    classpath_resources = [
        "//server/conf:logback.xml",
        "//server/conf:typedb-properties"
    ],
    data = [ "//server/conf:typedb-properties" ], # Separately needed for com.vaticle.typedb.core.server.common.Util
)

java_binary( # TODO: this target should not be needed if :server-deps-windows can depend directly on :server-windows (lib)
    name = "server-bin-deps-windows",
    main_class = "com.vaticle.typedb.core.server.TypeDBServer",
    runtime_deps = [":server-windows"],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-server-bin-deps:{pom_version}"],
)

java_deps(
    name = "server-deps-mac",
    target = ":server-bin-deps-mac",
    java_deps_root = "server/lib/common/",
    java_deps_root_overrides = {
        "rocksdbjni-dev-*": "server/lib/dev/",
    },
    visibility = ["//:__pkg__"],
    maven_name = True,
)

java_deps(
    name = "server-deps-linux",
    target = ":server-bin-deps-linux",
    java_deps_root = "server/lib/common/",
#    java_deps_root_overrides = {
#        "rocksdbjni-dev-*": "server/lib/dev/",
#    },
    visibility = ["//:__pkg__"],
    maven_name = True,
)

java_deps(
    name = "server-deps-windows",
    target = ":server-bin-deps-windows",
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
    "//server/conf:logback": "server/conf/logback.xml",
    "//server/conf:logback-debug": "server/conf/logback-debug.xml",
    "//server/conf:typedb-properties": "server/conf/typedb.properties",
    "//server/resources:logo": "server/resources/typedb-ascii.txt",
    "//:LICENSE": "LICENSE",
}

permissions = {
    "server/conf/typedb.properties": "0755",
    "server/conf/logback.xml": "0755",
    "server/conf/logback-debug.xml": "0755",
}

assemble_targz(
    name = "assemble-linux-targz",
    targets = ["server-deps-linux", "@vaticle_typedb_common//binary:assemble-bash-targz"],
    additional_files = assemble_files,
    permissions = permissions,
    output_filename = "typedb-server-linux",
    visibility = ["//:__pkg__", "//test:__subpackages__"]
)

assemble_zip(
    name = "assemble-mac-zip",
    targets = assemble_deps_common + ["server-deps-mac", "@vaticle_typedb_common//binary:assemble-bash-targz"],
    additional_files = assemble_files,
    permissions = permissions,
    output_filename = "typedb-server-mac",
    visibility = ["//:__pkg__", "//test:__subpackages__"]
)

assemble_zip(
    name = "assemble-windows-zip",
    targets = ["server-deps-windows", "@vaticle_typedb_common//binary:assemble-bat-targz"],
    additional_files = assemble_files,
    permissions = permissions,
    output_filename = "typedb-server-windows",
    visibility = ["//:__pkg__", "//test:__subpackages__"]
)

deploy_artifact(
    name = "deploy-linux-targz",
    target = ":assemble-linux-targz",
    artifact_group = "vaticle_typedb",
    artifact_name = "typedb-server-linux-{version}.tar.gz",
    release = deployment['artifact.release'],
    snapshot = deployment['artifact.snapshot'],
)

deploy_artifact(
    name = "deploy-mac-zip",
    target = ":assemble-mac-zip",
    artifact_group = "vaticle_typedb",
    artifact_name = "typedb-server-mac-{version}.zip",
    release = deployment['artifact.release'],
    snapshot = deployment['artifact.snapshot'],
)

deploy_artifact(
    name = "deploy-windows-zip",
    target = ":assemble-windows-zip",
    artifact_group = "vaticle_typedb",
    artifact_name = "typedb-server-windows-{version}.zip",
    release = deployment['artifact.release'],
    snapshot = deployment['artifact.snapshot'],
)

assemble_apt(
    name = "assemble-linux-apt",
    package_name = "typedb-server",
    maintainer = "Vaticle <community@vaticle.com>",
    description = "TypeDB (server)",
    depends = [
      "openjdk-11-jre",
      "typedb-bin (=%{@vaticle_typedb_common})"
    ],
    workspace_refs = "@vaticle_typedb_workspace_refs//:refs.json",
    archives = [":server-deps-linux"],
    installation_dir = "/opt/typedb/core/",
    files = assemble_files,
    empty_dirs = [
        "opt/typedb/core/server/lib/dev",
        "opt/typedb/core/server/lib/prod",
        "opt/typedb/core/server/lib/common",
        "var/lib/typedb/core/data/"
     ],
    permissions = {
        "var/lib/typedb/core/data/": "0777",
    },
    symlinks = {
        "opt/typedb/core/server/data": "/var/lib/typedb/core/data/",
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

