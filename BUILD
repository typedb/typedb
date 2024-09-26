# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//distribution/artifact:rules.bzl", "artifact_repackage")
load("//:deployment.bzl", deployment_docker = "deployment", deployment_github = "deployment")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//tool/release/deps:rules.bzl", "release_validate_deps")

load("@vaticle_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@vaticle_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_versioned", "assemble_zip")
load("@vaticle_bazel_distribution//platform:constraints.bzl", "constraint_linux_arm64", "constraint_linux_x86_64",
     "constraint_mac_arm64", "constraint_mac_x86_64", "constraint_win_x86_64")

load("@io_bazel_rules_docker//container:image.bzl", docker_container_image = "container_image")
load("@io_bazel_rules_docker//container:container.bzl", docker_container_push = "container_push")

load("@rules_pkg//:mappings.bzl", "pkg_files")
load("@rules_pkg//:pkg.bzl", "pkg_tar")
load("@rules_rust//rust:defs.bzl", "rust_binary")

exports_files(
    ["VERSION", "deployment.bzl", "LICENSE", "README.md"],
)

rust_binary(
    name = "typedb_server_bin",
    srcs = [
        "main.rs",
    ],
    deps = [
        "//common/logger",
        "//database",
        "//resource",
        "//server",

        "@crates//:tokio",
    ],
)

# Assembly
assemble_files = {
    "//:LICENSE": "LICENSE",
}
empty_directories = [
    "server/data",
]

permissions = {
    "server/conf/config.yml": "0755",
    "server/data": "0755",
    "server/typedb_server_bin": "0755",
    "typedb":  "0755",
}

alias(
    name = "typedb_console_artifact",
    actual = select({
        "@vaticle_bazel_distribution//platform:is_linux_arm64" : "@vaticle_typedb_console_artifact_linux-arm64//file",
        "@vaticle_bazel_distribution//platform:is_linux_x86_64" : "@vaticle_typedb_console_artifact_linux-x86_64//file",
        "@vaticle_bazel_distribution//platform:is_mac_arm64" : "@vaticle_typedb_console_artifact_mac-arm64//file",
        "@vaticle_bazel_distribution//platform:is_mac_x86_64" : "@vaticle_typedb_console_artifact_mac-x86_64//file",
        #"@vaticle_bazel_distribution//platform:is_windows_x86_64" : "@vaticle_typedb_console_artifact_windows-x86_64//file",
    })
)


artifact_repackage(
    name = "console-repackaged",
    srcs = [":typedb_console_artifact"],
    files_to_keep = ["console"],
)

pkg_files(
    name = "package-server-and-entry",
    srcs = ["//:typedb_server_bin", "//binary:typedb"],
    renames = {"//:typedb_server_bin" : "server/typedb_server_bin"}
)

pkg_tar(
    name = "package-typedb",
    srcs = [":package-server-and-entry"],
    deps = [":console-repackaged"],
)

assemble_zip(
    name = "assemble-mac-x86_64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-mac-x86_64",
    permissions = permissions,
    targets = ["//:package-typedb"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_mac_x86_64,
)

assemble_zip(
    name = "assemble-mac-arm64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-mac-arm64",
    permissions = permissions,
    targets = ["//:package-typedb"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_mac_arm64,
)

assemble_targz(
    name = "assemble-linux-x86_64-targz",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-linux-x86_64",
    permissions = permissions,
    targets = ["//:package-typedb"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_linux_x86_64,
)

assemble_targz(
    name = "assemble-linux-arm64-targz",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-linux-arm64",
    permissions = permissions,
    targets = ["//:package-typedb"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_linux_arm64,
)

deploy_artifact(
    name = "deploy-mac-x86_64-zip",
    artifact_group = "typedb-all-mac-x86_64",
    artifact_name = "typedb-all-mac-x86_64-{version}.zip",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-mac-x86_64-zip",
)

deploy_artifact(
    name = "deploy-mac-arm64-zip",
    artifact_group = "typedb-all-mac-arm64",
    artifact_name = "typedb-all-mac-arm64-{version}.zip",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-mac-arm64-zip",
)

deploy_artifact(
    name = "deploy-linux-x86_64-targz",
    artifact_group = "typedb-all-linux-x86_64",
    artifact_name = "typedb-all-linux-x86_64-{version}.tar.gz",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-linux-x86_64-targz",
)

deploy_artifact(
    name = "deploy-linux-arm64-targz",
    artifact_group = "typedb-all-linux-arm64",
    artifact_name = "typedb-all-linux-arm64-{version}.tar.gz",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-linux-arm64-targz",
)

# Convenience
alias(
    name = "assemble-typedb-server",
    actual = select({
        "@vaticle_bazel_distribution//platform:is_linux_arm64" : ":assemble-linux-arm64-targz",
        "@vaticle_bazel_distribution//platform:is_linux_x86_64" : ":assemble-linux-x86_64-targz",
        "@vaticle_bazel_distribution//platform:is_mac_arm64" : ":assemble-mac-arm64-zip",
        "@vaticle_bazel_distribution//platform:is_mac_x86_64" : ":assemble-mac-x86_64-zip",
#        "@vaticle_bazel_distribution//platform:is_windows_x86_64" : ":assemble-windows-x86_64-zip"
    }),
    visibility = ["//tests/assembly:__subpackages__"],
)
alias(
    name = "deploy-typedb-server",
    actual = select({
        "@vaticle_bazel_distribution//platform:is_linux_arm64" : ":deploy-linux-arm64-targz",
        "@vaticle_bazel_distribution//platform:is_linux_x86_64" : ":deploy-linux-x86_64-targz",
        "@vaticle_bazel_distribution//platform:is_mac_arm64" : ":deploy-mac-arm64-zip",
        "@vaticle_bazel_distribution//platform:is_mac_x86_64" : ":deploy-mac-x86_64-zip",
#        "@vaticle_bazel_distribution//platform:is_windows_x86_64" : ":deploy-windows-x86_64-zip"
    })
)

# docker
docker_container_image(
    name = "assemble-docker-x86_64",
    operating_system = "linux",
    architecture = "amd64",
    base = "@ubuntu-22.04-x86_64//image",
    cmd = [
        "/opt/typedb-all-linux-x86_64/typedb_server_bin"
    ],
    directory = "opt",
    env = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    },
    ports = ["1729"],
    tars = [":assemble-linux-x86_64-targz"],
    visibility = ["//test:__subpackages__"],
    volumes = ["/opt/typedb-all-linux-x86_64/server/data/"],
    workdir = "/opt/typedb-all-linux-x86_64",
    target_compatible_with = constraint_linux_x86_64,
)

docker_container_image(
    name = "assemble-docker-arm64",
    operating_system = "linux",
    architecture = "arm64",
    base = "@ubuntu-22.04-arm64//image",
    cmd = [
        "/opt/typedb-all-linux-arm64/typedb_server_bin"
    ],
    directory = "opt",
    env = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    },
    ports = ["1729"],
    tars = [":assemble-linux-arm64-targz"],
    visibility = ["//test:__subpackages__"],
    volumes = ["/opt/typedb-all-linux-arm64/server/data/"],
    workdir = "/opt/typedb-all-linux-arm64",
    target_compatible_with = constraint_linux_arm64,
)

docker_container_push(
    name = "deploy-docker-release-x86_64",
    format = "Docker",
    image = ":assemble-docker-x86_64",
    registry = deployment_docker["docker.index"],
    repository = "{}/{}".format(
        deployment_docker["docker.organisation"],
        deployment_docker["docker.release.repository"],
    ),
    tag_file = "//docker:version-x86_64",
    target_compatible_with = constraint_linux_x86_64,
)

docker_container_push(
    name = "deploy-docker-release-arm64",
    format = "Docker",
    image = ":assemble-docker-arm64",
    registry = deployment_docker["docker.index"],
    repository = "{}/{}".format(
        deployment_docker["docker.organisation"],
        deployment_docker["docker.release.repository"],
    ),
    tag_file = "//docker:version-arm64",
    target_compatible_with = constraint_linux_arm64,
)

# validation & tests
release_validate_deps(
    name = "release-validate-deps",
    refs = "@vaticle_typedb_workspace_refs//:refs.json",
    tagged_deps = [
        # TODO: Reenable
        # "@vaticle_typeql",
         "@vaticle_typedb_protocol",
    ],
    tags = ["manual"],  # in order for bazel test //... to not fail
    version_file = "VERSION",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", ".factory/*", "bin/*", ".circleci/*"]),
    exclude = glob([
        "*.md",
        ".circleci/windows/*",
        "docs/*",
        "tools/**",
        "Cargo.*",
    ]) + [
        ".bazelversion",
        ".bazel-remote-cache.rc",
        ".bazel-cache-credential.json",
        "LICENSE",
        "VERSION",
    ],
    license_type = "mpl-header",
)

checkstyle_test(
    name = "checkstyle_license",
    include = ["LICENSE"],
    license_type = "mpl-fulltext",
)

filegroup(
    name = "tools",
    data = [
        "@vaticle_dependencies//factory/analysis:dependency-analysis",
        "@vaticle_dependencies//tool/bazelinstall:remote_cache_setup.sh",
        "@vaticle_dependencies//tool/release/notes:create",
        "@vaticle_dependencies//tool/checkstyle:test-coverage",
        "@vaticle_dependencies//tool/unuseddeps:unused-deps",
        "@rust_analyzer_toolchain_tools//lib/rustlib/src:rustc_srcs",
        "@vaticle_dependencies//tool/ide:rust_sync",
    ],
)
