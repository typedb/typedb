# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//distribution:deployment.bzl", "deployment")
load("@typedb_dependencies//distribution/artifact:rules.bzl", "artifact_repackage")
load("//:deployment.bzl", deployment_docker = "deployment", deployment_github = "deployment")
load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@typedb_dependencies//tool/release/deps:rules.bzl", "release_validate_deps")

load("@typedb_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@typedb_bazel_distribution//common:rules.bzl", "assemble_targz", "assemble_versioned", "assemble_zip")
load("@typedb_bazel_distribution//platform:constraints.bzl", "constraint_linux_arm64", "constraint_linux_x86_64",
     "constraint_mac_arm64", "constraint_mac_x86_64", "constraint_win_x86_64")

load("@io_bazel_rules_docker//container:image.bzl", docker_container_image = "container_image")
load("@io_bazel_rules_docker//container:container.bzl", docker_container_push = "container_push")

load("@rules_pkg//:mappings.bzl", "pkg_files", "pkg_attributes")
load("@rules_pkg//:pkg.bzl", "pkg_tar")
load("@rules_rust//rust:defs.bzl", "rust_binary")

load("@typedb_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@typedb_bazel_distribution//brew:rules.bzl", "deploy_brew")

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

        "@crates//:clap",
        "@crates//:sentry",
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

binary_permissions = pkg_attributes(mode = "0744")
other_permissions = {
    "server/data": "0755",
}

alias(
    name = "typedb_console_artifact",
    actual = select({
        "@typedb_bazel_distribution//platform:is_linux_arm64" : "@typedb_console_artifact_linux-arm64//file",
        "@typedb_bazel_distribution//platform:is_linux_x86_64" : "@typedb_console_artifact_linux-x86_64//file",
        "@typedb_bazel_distribution//platform:is_mac_arm64" : "@typedb_console_artifact_mac-arm64//file",
        "@typedb_bazel_distribution//platform:is_mac_x86_64" : "@typedb_console_artifact_mac-x86_64//file",
        "@typedb_bazel_distribution//platform:is_windows_x86_64" : "@typedb_console_artifact_windows-x86_64//file",
    })
)

# The directory structure for distribution
pkg_files(
    name = "package-layout-server",
    srcs = ["//:typedb_server_bin", "//binary:typedb"],
    renames = {"//:typedb_server_bin" : "server/typedb_server_bin"},
    attributes = binary_permissions,
)

pkg_tar(
    name = "package-typedb-server-only",
    srcs = [":package-layout-server"],
)

assemble_zip(
    name = "assemble-server-mac-x86_64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-server-mac-x86_64",
    permissions = other_permissions,
    targets = ["//:package-typedb-server-only"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_mac_x86_64,
)

assemble_zip(
    name = "assemble-server-mac-arm64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-server-mac-arm64",
    permissions = other_permissions,
    targets = ["//:package-typedb-server-only"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_mac_arm64,
)

assemble_targz(
    name = "assemble-server-linux-x86_64-targz",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-server-linux-x86_64",
    permissions = other_permissions,
    targets = ["//:package-typedb-server-only"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_linux_x86_64,
)

assemble_targz(
    name = "assemble-server-linux-arm64-targz",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-server-linux-arm64",
    permissions = other_permissions,
    targets = ["//:package-typedb-server-only"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_linux_arm64,
)

assemble_zip(
    name = "assemble-server-windows-x86_64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-server-windows-x86_64",
    permissions = other_permissions,
    targets = ["//:package-typedb-server-only"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_win_x86_64,
)

# Package TypeDB & console together
artifact_repackage(
    name = "console-repackaged",
    srcs = [":typedb_console_artifact"],
    files_to_keep = ["console"],
)

pkg_tar(
    name = "package-typedb-all",
    srcs = [":package-layout-server"],
    deps = [":console-repackaged"],
)

assemble_zip(
    name = "assemble-all-mac-x86_64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-mac-x86_64",
    permissions = other_permissions,
    targets = ["//:package-typedb-all"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_mac_x86_64,
)

assemble_zip(
    name = "assemble-all-mac-arm64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-mac-arm64",
    permissions = other_permissions,
    targets = ["//:package-typedb-all"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_mac_arm64,
)

assemble_targz(
    name = "assemble-all-linux-x86_64-targz",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-linux-x86_64",
    permissions = other_permissions,
    targets = ["//:package-typedb-all"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_linux_x86_64,
)

assemble_targz(
    name = "assemble-all-linux-arm64-targz",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-linux-arm64",
    permissions = other_permissions,
    targets = ["//:package-typedb-all"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_linux_arm64,
)

assemble_zip(
    name = "assemble-all-windows-x86_64-zip",
    additional_files = assemble_files,
    empty_directories = empty_directories,
    output_filename = "typedb-all-windows-x86_64",
    permissions = other_permissions,
    targets = ["//:package-typedb-all"],
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_win_x86_64,
)

deploy_artifact(
    name = "deploy-mac-x86_64-zip",
    artifact_group = "typedb-all-mac-x86_64",
    artifact_name = "typedb-all-mac-x86_64-{version}.zip",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-all-mac-x86_64-zip",
)

deploy_artifact(
    name = "deploy-mac-arm64-zip",
    artifact_group = "typedb-all-mac-arm64",
    artifact_name = "typedb-all-mac-arm64-{version}.zip",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-all-mac-arm64-zip",
)

deploy_artifact(
    name = "deploy-linux-x86_64-targz",
    artifact_group = "typedb-all-linux-x86_64",
    artifact_name = "typedb-all-linux-x86_64-{version}.tar.gz",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-all-linux-x86_64-targz",
)

deploy_artifact(
    name = "deploy-linux-arm64-targz",
    artifact_group = "typedb-all-linux-arm64",
    artifact_name = "typedb-all-linux-arm64-{version}.tar.gz",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-all-linux-arm64-targz",
)

deploy_artifact(
    name = "deploy-windows-x86_64-zip",
    artifact_group = "typedb-all-windows-x86_64",
    artifact_name = "typedb-all-windows-x86_64-{version}.zip",
    release = deployment["artifact"]["release"]["upload"],
    snapshot = deployment["artifact"]["snapshot"]["upload"],
    target = ":assemble-all-windows-x86_64-zip",
)

# Convenience
alias(
    name = "assemble-typedb-all",
    actual = select({
        "@typedb_bazel_distribution//platform:is_linux_arm64" : ":assemble-all-linux-arm64-targz",
        "@typedb_bazel_distribution//platform:is_linux_x86_64" : ":assemble-all-linux-x86_64-targz",
        "@typedb_bazel_distribution//platform:is_mac_arm64" : ":assemble-all-mac-arm64-zip",
        "@typedb_bazel_distribution//platform:is_mac_x86_64" : ":assemble-all-mac-x86_64-zip",
        "@typedb_bazel_distribution//platform:is_windows_x86_64" : ":assemble-all-windows-x86_64-zip"
    }),
    visibility = ["//tests/assembly:__subpackages__"],
)
alias(
    name = "deploy-typedb-server",
    actual = select({
        "@typedb_bazel_distribution//platform:is_linux_arm64" : ":deploy-linux-arm64-targz",
        "@typedb_bazel_distribution//platform:is_linux_x86_64" : ":deploy-linux-x86_64-targz",
        "@typedb_bazel_distribution//platform:is_mac_arm64" : ":deploy-mac-arm64-zip",
        "@typedb_bazel_distribution//platform:is_mac_x86_64" : ":deploy-mac-x86_64-zip",
        "@typedb_bazel_distribution//platform:is_windows_x86_64" : ":deploy-windows-x86_64-zip"
    })
)

# docker
docker_container_image(
    name = "assemble-docker-x86_64",
    operating_system = "linux",
    architecture = "amd64",
    base = "@typedb-ubuntu-x86_64//image",
    cmd = ["/opt/typedb-server-linux-x86_64/typedb", "server"],
    directory = "opt",
    env = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    },
    ports = ["1729"],
    tars = [":assemble-server-linux-x86_64-targz"],
    visibility = ["//test:__subpackages__"],
    volumes = ["/opt/typedb-server-linux-x86_64/server/data/"],
    workdir = "/opt/typedb-server-linux-x86_64",
    target_compatible_with = constraint_linux_x86_64,
)

docker_container_image(
    name = "assemble-docker-arm64",
    operating_system = "linux",
    architecture = "arm64",
    base = "@typedb-ubuntu-arm64//image",
    cmd = ["/opt/typedb-server-linux-arm64/typedb", "server"],
    directory = "opt",
    env = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    },
    ports = ["1729"],
    tars = [":assemble-server-linux-arm64-targz"],
    visibility = ["//test:__subpackages__"],
    volumes = ["/opt/typedb-server-linux-arm64/server/data/"],
    workdir = "/opt/typedb-server-linux-arm64",
    target_compatible_with = constraint_linux_arm64,
)

# TODO: Make a typedb-distribution rule similar to `deploy_apt`

docker_container_push(
    name = "deploy-docker-snapshot-x86_64",
    format = "Docker",
    image = ":assemble-docker-x86_64",
    registry = deployment_docker["docker.index"],
    repository = "{}/{}".format(
        deployment_docker["docker.organisation"],
        deployment_docker["docker.snapshot.repository"],
    ),
    # using $(version) propagates to `assemble-docker` and breaks the image so it doesn't boot
    tag = "$(container-version)",
    target_compatible_with = constraint_linux_x86_64,
    tags = ["manual"],
)

docker_container_push(
    name = "deploy-docker-snapshot-arm64",
    format = "Docker",
    image = ":assemble-docker-arm64",
    registry = deployment_docker["docker.index"],
    repository = "{}/{}".format(
        deployment_docker["docker.organisation"],
        deployment_docker["docker.snapshot.repository"],
    ),
    tag = "$(container-version)",
    target_compatible_with = constraint_linux_arm64,
    tags = ["manual"],
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

# brew
deploy_brew(
    name = "deploy-brew",
    file_substitutions = {
        "//:checksum-mac-arm64": "{sha256-arm64}",
        "//:checksum-mac-x86_64": "{sha256-x86_64}",
    },
    formula = "//config/brew:typedb.rb",
    release = deployment["brew"]["release"],
    snapshot = deployment["brew"]["snapshot"],
    version_file = "//:VERSION",
)

genrule(
    name = "invalid-checksum",
    outs = ["invalid-checksum.txt"],
    srcs = [],
    cmd = "echo > $@",
)

label_flag(
    name = "checksum-mac-arm64",
    build_setting_default = ":invalid-checksum",
)

label_flag(
    name = "checksum-mac-x86_64",
    build_setting_default = ":invalid-checksum",
)

# apt
apt_depends = []

apt_installation_dir = "/opt/typedb/core/"

apt_empty_dirs = [
    "/var/log/typedb/",
    "/var/lib/typedb/core/data/",
]

apt_symlinks = {
    "/opt/typedb/core/server/data": "/var/lib/typedb/core/data/",
    "/usr/local/bin/typedb": "/opt/typedb/core/typedb",
    "/opt/typedb/core/server/logs": "/var/log/typedb/",
    "/usr/lib/systemd/system/typedb.service": "/opt/typedb/core/typedb.service",
}

assemble_targz(
    name = "assemble-service-targz",
    additional_files = {
        "//binary:typedb.service": "typedb.service",
    },
    visibility = ["//visibility:public"]
)

assemble_apt(
    name = "assemble-linux-x86_64-apt",
    package_name = "typedb",
    architecture = "amd64",
    archives = [
        "//:package-typedb-all",
        "//:assemble-service-targz",
    ],
    depends = apt_depends,
    description = "TypeDB",
    empty_dirs = apt_empty_dirs,
    empty_dirs_permission = "0777",
    files = assemble_files,
    installation_dir = apt_installation_dir,
    maintainer = "TypeDB Community <community@typedb.com>",
    symlinks = apt_symlinks,
    workspace_refs = "@typedb_workspace_refs//:refs.json",
)

deploy_apt(
    name = "deploy-apt-x86_64",
    release = deployment["apt"]["release"]["upload"],
    snapshot = deployment["apt"]["snapshot"]["upload"],
    target = ":assemble-linux-x86_64-apt",
)

assemble_apt(
    name = "assemble-linux-arm64-apt",
    package_name = "typedb",
    architecture = "arm64",
    archives = [
        "//:package-typedb-all",
        "//:assemble-service-targz",
    ],
    depends = apt_depends,
    description = "TypeDB",
    empty_dirs = apt_empty_dirs,
    empty_dirs_permission = "0777",
    files = assemble_files,
    installation_dir = apt_installation_dir,
    maintainer = "TypeDB Community <community@typedb.com>",
    symlinks = apt_symlinks,
    workspace_refs = "@typedb_workspace_refs//:refs.json",
)

deploy_apt(
    name = "deploy-apt-arm64",
    release = deployment["apt"]["release"]["upload"],
    snapshot = deployment["apt"]["snapshot"]["upload"],
    target = ":assemble-linux-arm64-apt",
)

alias(
    name = "deploy-apt",
    actual = select({
        "@typedb_bazel_distribution//platform:is_linux_arm64" : ":deploy-apt-arm64",
        "@typedb_bazel_distribution//platform:is_linux_x86_64" : ":deploy-apt-x86_64",
    }),
    tags = ["manual"],
)

# validation & tests
release_validate_deps(
    name = "release-validate-deps",
    refs = "@typedb_workspace_refs//:refs.json",
    tagged_deps = [
        # TODO: Reenable
#         "@typeql",
         "@typedb_protocol",
    ],
    tags = ["manual"],  # in order for bazel test //... to not fail
    version_file = "VERSION",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", ".cargo/*", ".factory/*", "bin/*", ".circleci/*"]),
    exclude = glob([
        "*.md",
        ".circleci/windows/*",
        "docs/**",
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
        "@typedb_dependencies//factory/analysis:dependency-analysis",
        "@typedb_dependencies//tool/bazelinstall:remote_cache_setup.sh",
        "@typedb_dependencies//tool/release/notes:create",
        "@typedb_dependencies//tool/checkstyle:test-coverage",
        "@typedb_dependencies//tool/unuseddeps:unused-deps",
        "@rust_analyzer_toolchain_tools//lib/rustlib/src:rustc_srcs",
        "@typedb_dependencies//tool/ide:rust_sync",
    ],
)
