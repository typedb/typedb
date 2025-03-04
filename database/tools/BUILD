# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@rules_rust//rust:defs.bzl", "rust_binary")
load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")

rust_binary(
    name = "read_wal",
    srcs = [
        "read_wal.rs",
    ],
    deps = [
        "//concept",
        "//durability",
        "//storage",
        "@crates//:clap",
    ],
)

rust_binary(
    name = "replay_wal",
    srcs = [
        "replay_wal.rs",
    ],
    deps = [
        "//concept",
        "//durability",
        "//storage",
        "@crates//:clap",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
