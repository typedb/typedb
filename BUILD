# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//distribution:deployment.bzl", "deployment")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@vaticle_dependencies//tool/release/deps:rules.bzl", "release_validate_deps")

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
