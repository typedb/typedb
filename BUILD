# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//distribution:deployment.bzl", "deployment")
load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@typedb_dependencies//tool/release/deps:rules.bzl", "release_validate_deps")

load("@typedb_bazel_distribution//artifact:rules.bzl", "deploy_artifact")
load("@typedb_dependencies//distribution/artifact:rules.bzl", "artifact_repackage")
load("@typedb_bazel_distribution//platform:constraints.bzl", "constraint_linux_arm64", "constraint_linux_x86_64",
     "constraint_mac_arm64", "constraint_mac_x86_64", "constraint_win_x86_64")

load("@bazel_skylib//rules:select_file.bzl", "select_file")
load("@rules_oci//oci:defs.bzl", "oci_image", "oci_push")

load("@rules_pkg//:mappings.bzl", "pkg_attributes" , "pkg_files", "pkg_filegroup", "pkg_mkdirs")
load("@rules_pkg//:pkg.bzl", "pkg_tar", "pkg_zip")
load("@rules_rust//rust:defs.bzl", "rust_binary", "rustfmt_test")

load("@typedb_bazel_distribution//apt:rules.bzl", "assemble_apt", "deploy_apt")
load("@typedb_bazel_distribution//brew:rules.bzl", "deploy_brew")

exports_files(
    ["VERSION", "deployment.bzl", "LICENSE", "README.md"],
)

genrule(
    name = "config_at_root",
    outs = ["config.yml"],
    srcs = ["//server:config.yml"],
    cmd = "cp $(location //server:config.yml) $(location config.yml)",
)

rust_binary(
    name = "typedb_server_bin",
    srcs = [
        "main.rs",
    ],
    deps = [
        "//common/fail_point",
        "//common/logger",
        "//database",
        "//resource",
        "//server",

        "@crates//:clap",
        "@crates//:sentry",
        "@crates//:tokio",
        "@crates//:tracing",
    ],
    data = ["//:config_at_root"]
)

# Assembly
pkg_mkdirs(
    name = "pkg-empty-server-data",
    dirs = ["server/data"],
    attributes = pkg_attributes(mode = "0755"),
)

binary_permissions = pkg_attributes(mode = "0744")

alias(
    name = "typedb_console_artifact_extracted",
    actual = select({
        "@typedb_bazel_distribution//platform:is_linux_arm64" : "@typedb_console_artifact_linux-arm64-extracted//:all_files",
        "@typedb_bazel_distribution//platform:is_linux_x86_64" : "@typedb_console_artifact_linux-x86_64-extracted//:all_files",
        "@typedb_bazel_distribution//platform:is_mac_arm64" : "@typedb_console_artifact_mac-arm64-extracted//:all_files",
        "@typedb_bazel_distribution//platform:is_mac_x86_64" : "@typedb_console_artifact_mac-x86_64-extracted//:all_files",
        "@typedb_bazel_distribution//platform:is_windows_x86_64" : "@typedb_console_artifact_windows-x86_64-extracted//:all_files",
    })
)

# The directory structure for distribution (Unix)
pkg_files(
    name = "package-layout-server-without-dirs",
    srcs = ["//:typedb_server_bin", "//admin:typedb_admin_bin", "//binary:typedb", "//server:config.yml", "//:LICENSE", ":pkg-empty-server-data"],
    renames = {
        "//:typedb_server_bin" : "server/typedb_server_bin",
        "//admin:typedb_admin_bin" : "admin/typedb_admin_bin",
        "//server:config.yml" : "server/config.yml",
    },
    attributes = binary_permissions,
)

pkg_filegroup(
    name = "package-server-only",
    srcs = [":package-layout-server-without-dirs", ":pkg-empty-server-data"]
)

pkg_zip(
    name = "assemble-server-mac-x86_64-zip",
    srcs = [":package-server-only"],
    package_dir = "typedb-server-mac-x86_64",
    out = "typedb-server-mac-x86_64.zip",
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_mac_x86_64,
)

pkg_zip(
    name = "assemble-server-mac-arm64-zip",
    srcs = [":package-server-only"],
    package_dir = "typedb-server-mac-arm64",
    out = "typedb-server-mac-arm64.zip",
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_mac_arm64,
)


pkg_tar(
    name = "assemble-server-linux-x86_64-targz",
    srcs = [":package-server-only"],
    package_dir = "typedb-server-linux-x86_64",
    out = "typedb-server-linux-x86_64.tar.gz",
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_linux_x86_64,
)

pkg_tar(
    name = "assemble-server-linux-arm64-targz",
    srcs = [":package-server-only"],
    package_dir = "typedb-server-linux-arm64",
    out = "typedb-server-linux-arm64.tar.gz",
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_linux_arm64,
)

pkg_zip(
    name = "assemble-server-windows-x86_64-zip",
    srcs = [":package-server-only"],
    package_dir = "typedb-server-windows-x86_64",
    out = "typedb-server-windows-x86_64.zip",
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_win_x86_64,
)

#
# rules_pkg-only assembly — no assemble_zip

select_file(
    name = "console-binary-only",
    srcs = ":typedb_console_artifact_extracted",
    subpath = "console/typedb_console_bin",
)

pkg_files(
    name = "console-repackaged",
    srcs = [":console-binary-only"],
    prefix = "console",
    attributes = binary_permissions,
)

pkg_filegroup(
    name = "package-typedb-all",
    srcs = [
        ":package-server-only",
        ":console-repackaged",
    ],
)

pkg_zip(
    name = "assemble-all-mac-x86_64-zip",
    srcs = [":package-typedb-all"],
    package_dir = "typedb-all-mac-x86_64",
    out = "typedb-all-mac-x86_64.zip",
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_mac_x86_64,
)
#
pkg_zip(
    name = "assemble-all-mac-arm64-zip",
    srcs = [":package-typedb-all"],
    package_dir = "typedb-all-mac-arm64",
    out = "typedb-all-mac-arm64.zip",
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_mac_arm64,
)

pkg_tar(
    name = "assemble-all-linux-x86_64-targz",
    srcs = [":package-typedb-all"],
    package_dir = "typedb-all-linux-x86_64",
    out = "typedb-all-linux-x86_64.tar.gz",
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_linux_x86_64,
)

pkg_tar(
    name = "assemble-all-linux-arm64-targz",
    srcs = [":package-typedb-all"],
    package_dir = "typedb-all-linux-arm64",
    out = "typedb-all-linux-arm64.tar.gz",
    visibility = ["//tests/assembly:__subpackages__"],
    target_compatible_with = constraint_linux_arm64,
)

pkg_zip(
    name = "assemble-all-windows-x86_64-zip",
    srcs = [":package-typedb-all"],
    package_dir = "typedb-all-windows-x86_64",
    out = "typedb-all-windows-x86_64.zip",
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
pkg_tar(
    name = "docker-layer-x86_64",
    deps = [":assemble-server-linux-x86_64-targz"],
    package_dir = "/opt",
    extension = "tar.gz",
)

oci_image(
    name = "assemble-docker-x86_64",
    base = "@typedb-ubuntu-x86_64",
    tars = [":docker-layer-x86_64"],
    entrypoint = ["/opt/typedb-server-linux-x86_64/typedb", "server", "--storage.data-directory=/var/lib/typedb/data"],
    cmd = [],
    env = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    },
    exposed_ports = ["1729/tcp", "8000/tcp"],
    volumes = ["/var/lib/typedb/data/"],
    workdir = "/opt/typedb-server-linux-x86_64",
    visibility = ["//test:__subpackages__"],
    target_compatible_with = constraint_linux_x86_64,
)

pkg_tar(
    name = "docker-layer-arm64",
    deps = [":assemble-server-linux-arm64-targz"],
    package_dir = "/opt",
    extension = "tar.gz",
)

oci_image(
    name = "assemble-docker-arm64",
    base = "@typedb-ubuntu-arm64",
    tars = [":docker-layer-arm64"],
    entrypoint = ["/opt/typedb-server-linux-arm64/typedb", "server", "--storage.data-directory=/var/lib/typedb/data"],
    cmd = [],
    env = {
        "LANG": "C.UTF-8",
        "LC_ALL": "C.UTF-8",
    },
    exposed_ports = ["1729/tcp", "8000/tcp"],
    volumes = ["/var/lib/typedb/data/"],
    workdir = "/opt/typedb-server-linux-arm64",
    visibility = ["//test:__subpackages__"],
    target_compatible_with = constraint_linux_arm64,
)

oci_push(
    name = "deploy-docker-snapshot-x86_64",
    image = ":assemble-docker-x86_64",
    repository = "index.docker.io/typedb/typedb-snapshot",
    target_compatible_with = constraint_linux_x86_64,
    tags = ["manual"],
)

oci_push(
    name = "deploy-docker-snapshot-arm64",
    image = ":assemble-docker-arm64",
    repository = "index.docker.io/typedb/typedb-snapshot",
    target_compatible_with = constraint_linux_arm64,
    tags = ["manual"],
)

oci_push(
    name = "deploy-docker-release-x86_64",
    image = ":assemble-docker-x86_64",
    repository = "index.docker.io/typedb/typedb",
    remote_tags = "//docker:version-x86_64",
    target_compatible_with = constraint_linux_x86_64,
)

oci_push(
    name = "deploy-docker-release-arm64",
    image = ":assemble-docker-arm64",
    repository = "index.docker.io/typedb/typedb",
    remote_tags = "//docker:version-arm64",
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

pkg_tar(
    name = "assemble-service-targz",
    srcs = ["//binary:typedb.service"],
    visibility = ["//visibility:public"]
)
pkg_tar(
    name = "package-typedb-all-tarred",
    srcs = [":package-typedb-all"]
)

assemble_apt(
    name = "assemble-linux-x86_64-apt",
    package_name = "typedb",
    architecture = "amd64",
    archives = [
        "//:package-typedb-all-tarred",
        "//:assemble-service-targz",
    ],
    depends = apt_depends,
    description = "TypeDB",
    empty_dirs = apt_empty_dirs,
    empty_dirs_permission = "0777",
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
        "//:package-typedb-all-tarred",
        "//:assemble-service-targz",
    ],
    depends = apt_depends,
    description = "TypeDB",
    empty_dirs = apt_empty_dirs,
    empty_dirs_permission = "0777",
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
         "@typeql+",
         "@typedb_protocol+",
    ],
    tags = ["manual"],  # in order for bazel test //... to not fail
    version_file = "VERSION",
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [ ":typedb_server_bin" ],
    size = "small",
)

filegroup(
    name = "rustfmt_config",
    srcs = ["rustfmt.toml"],
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
        ".git",
        "LICENSE",
        "MODULE.bazel.lock",
        "VERSION",
        "WORKSPACE.bazel",
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
        # Note: @rust_analyzer_toolchain_tools removed - not automatically created
        # by Bzlmod extension, optional for IDE support
        "@typedb_dependencies//tool/ide:rust_sync",
    ],
)
