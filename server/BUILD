# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@vaticle_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_zip", "java_deps")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//builder/java:rules.bzl", "native_java_libraries")
load("@bazel_skylib//rules:common_settings.bzl", "string_flag")

package(default_visibility = ["//visibility:private"])

genrule(
    name = "version-src",
    srcs = [
        "templates/Version.java",
        "//:VERSION",
    ],
    outs = ["Version.java"],
    cmd = "VERSION=`cat $(location //:VERSION)`;sed -e \"s/{version}/$$VERSION/g\" $(location templates/Version.java) >> $@",
)

java_library(
    name = "version",
    srcs = [":version-src"],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-version:{pom_version}"],
    visibility = ["//test:__subpackages__"],
)

native_java_libraries(
    name = "server",
    srcs = glob(
        [
            "*.java",
            "*/*.java",
            "*/*/*.java",
        ],
        exclude = [
            "templates/**",
            "test/**",
        ],
    ),
    native_libraries_deps = [
        # Internal dependencies
        "//:typedb",
        "//common:common",
        "//concurrent:concurrent",
        "//concept:concept",
        "//logic:logic",
        "//query:query",
        "//database:database",
        "//reasoner:reasoner",
        "//migrator:migrator",
    ],
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-server:{pom_version}"],
    visibility = ["//visibility:public"],
    runtime_deps = [
        "@maven//:ch_qos_logback_logback_classic",
    ],
    deps = [
        ":version",

        # External dependencies from Vaticle
        "@vaticle_typeql//common/java:common",
        "@vaticle_typeql//java/pattern",
        "@vaticle_typeql//java:typeql-lang",
        "@vaticle_typeql//java/query",
        "@vaticle_typeql//java/common",
        "@vaticle_typedb_protocol//grpc/java:typedb-protocol",

        # External dependencies from Maven
        "@maven//:com_eclipsesource_minimal_json_minimal_json",
        "@maven//:com_google_code_findbugs_jsr305",
        "@maven//:com_google_protobuf_protobuf_java",
        "@maven//:io_grpc_grpc_api",
        "@maven//:io_grpc_grpc_netty",
        "@maven//:io_grpc_grpc_stub",
        "@maven//:io_netty_netty_all",
        "@maven//:io_netty_netty_handler",
        "@maven//:io_netty_netty_buffer",
        "@maven//:io_netty_netty_codec_http",
        "@maven//:io_netty_netty_transport",
        "@maven//:org_slf4j_slf4j_api",
        "@maven//:ch_qos_logback_logback_classic",
        "@maven//:ch_qos_logback_logback_core",
        "@maven//:io_sentry_sentry",
    ],
)

genrule(
    name = "prepare-server-directories",
    srcs = ["//server/parameters/config"],
    outs = [
        "conf/config.yml",
        "data/empty",
    ],
    # TODO: can we find a way to create a directory without including an empty file in the outs?
    cmd = "cp $(location //server/parameters/config) $(@D)/conf/ && touch $(@D)/data/empty",
)

java_binary(
    name = "server-bin-linux-arm64",
    data = [":prepare-server-directories"],
    main_class = "com.vaticle.typedb.core.server.TypeDBServer",
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-server-bin:{pom_version}"],
    runtime_deps = [":server-linux-arm64"],
)

java_binary(
    name = "server-bin-linux-x86_64",
    data = [":prepare-server-directories"],
    main_class = "com.vaticle.typedb.core.server.TypeDBServer",
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-server-bin:{pom_version}"],
    runtime_deps = [":server-linux-x86_64"],
)

java_binary(
    name = "server-bin-mac-arm64",
    data = [":prepare-server-directories"],
    main_class = "com.vaticle.typedb.core.server.TypeDBServer",
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-server-bin:{pom_version}"],
    runtime_deps = [":server-mac-arm64"],
)

java_binary(
    name = "server-bin-mac-x86_64",
    data = [":prepare-server-directories"],
    main_class = "com.vaticle.typedb.core.server.TypeDBServer",
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-server-bin:{pom_version}"],
    runtime_deps = [":server-mac-x86_64"],
)

java_binary(
    name = "server-bin-windows-x86_64",
    data = [":prepare-server-directories"],
    main_class = "com.vaticle.typedb.core.server.TypeDBServer",
    tags = ["maven_coordinates=com.vaticle.typedb:typedb-core-server-bin:{pom_version}"],
    runtime_deps = [":server-windows-x86_64"],
)

java_deps(
    name = "server-deps-linux-arm64",
    ignore_missing_maven_name = True,
    java_deps_root = "server/lib/",
    maven_name = True,
    target = ":server-bin-linux-arm64",
    visibility = ["//:__pkg__"],
)

java_deps(
    name = "server-deps-linux-x86_64",
    ignore_missing_maven_name = True,
    java_deps_root = "server/lib/",
    maven_name = True,
    target = ":server-bin-linux-x86_64",
    visibility = ["//:__pkg__"],
)

java_deps(
    name = "server-deps-mac-arm64",
    ignore_missing_maven_name = True,
    java_deps_root = "server/lib/",
    maven_name = True,
    target = ":server-bin-mac-arm64",
    visibility = ["//:__pkg__"],
)

java_deps(
    name = "server-deps-mac-x86_64",
    ignore_missing_maven_name = True,
    java_deps_root = "server/lib/",
    maven_name = True,
    target = ":server-bin-mac-x86_64",
    visibility = ["//:__pkg__"],
)

java_deps(
    name = "server-deps-windows-x86_64",
    ignore_missing_maven_name = True,
    java_deps_root = "server/lib/",
    maven_name = True,
    target = ":server-bin-windows-x86_64",
    visibility = ["//:__pkg__"],
)

assemble_files = {
    "//server/parameters/config": "server/conf/config.yml",
    "//server/resources:logo": "server/resources/typedb-ascii.txt",
    "//:LICENSE": "LICENSE",
}

empty_directories = [
    "server/data",
]

permissions = {
    "server/conf/config.yml": "0755",
    "server/data": "0755",
}

assemble_targz(
    name = "assemble-linux-arm64-targz",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-server-linux-arm64",
    permissions = permissions,
    targets = [
        "server-deps-linux-arm64",
        "//server/binary:assemble-bash-targz",
    ],
    visibility = [
        "//:__pkg__",
        "//test:__subpackages__",
    ],
)

assemble_targz(
    name = "assemble-linux-x86_64-targz",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-server-linux-x86_64",
    permissions = permissions,
    targets = [
        "server-deps-linux-x86_64",
        "//server/binary:assemble-bash-targz",
    ],
    visibility = [
        "//:__pkg__",
        "//test:__subpackages__",
    ],
)

assemble_zip(
    name = "assemble-mac-arm64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-server-mac-arm64",
    permissions = permissions,
    targets = [
        "server-deps-mac-arm64",
        "//server/binary:assemble-bash-targz",
    ],
    visibility = [
        "//:__pkg__",
        "//test:__subpackages__",
    ],
)

assemble_zip(
    name = "assemble-mac-x86_64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-server-mac-x86_64",
    permissions = permissions,
    targets = [
        "server-deps-mac-x86_64",
        "//server/binary:assemble-bash-targz",
    ],
    visibility = [
        "//:__pkg__",
        "//test:__subpackages__",
    ],
)

assemble_zip(
    name = "assemble-windows-x86_64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-server-windows-x86_64",
    permissions = permissions,
    targets = [
        "server-deps-windows-x86_64",
        "//server/binary:assemble-bat-targz",
    ],
    visibility = [
        "//:__pkg__",
        "//test:__subpackages__",
    ],
)

deploy_artifact(
    name = "deploy-linux-arm64-targz",
    artifact_group = "typedb-server-linux-arm64",
    artifact_name = "typedb-server-linux-arm64-{version}.tar.gz",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-linux-arm64-targz",
)

deploy_artifact(
    name = "deploy-linux-x86_64-targz",
    artifact_group = "typedb-server-linux-x86_64",
    artifact_name = "typedb-server-linux-x86_64-{version}.tar.gz",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-linux-x86_64-targz",
)

deploy_artifact(
    name = "deploy-mac-arm64-zip",
    artifact_group = "typedb-server-mac-arm64",
    artifact_name = "typedb-server-mac-arm64-{version}.zip",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-mac-arm64-zip",
)

deploy_artifact(
    name = "deploy-mac-x86_64-zip",
    artifact_group = "typedb-server-mac-x86_64",
    artifact_name = "typedb-server-mac-x86_64-{version}.zip",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-mac-x86_64-zip",
)

deploy_artifact(
    name = "deploy-windows-x86_64-zip",
    artifact_group = "typedb-server-windows-x86_64",
    artifact_name = "typedb-server-windows-x86_64-{version}.zip",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-windows-x86_64-zip",
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
        "*/*",
        "*/*/*",
    ]),
    license_type = "mpl-header",
)
