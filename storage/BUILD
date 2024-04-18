# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library")
package(default_visibility = [
    "//concept:__subpackages__",
    "//database:__subpackages__",
    "//encoding:__subpackages__",
    "//storage/tests:__subpackages__",
    "//storage/benches:__subpackages__",
])

rust_library(
    name = "storage",
    crate_root = "storage.rs",
    srcs = glob([
        "*.rs",
        "keyspace/*.rs",
        "snapshot/*.rs",
    ]),
    deps = [
        "//storage/durability",
        "//common/logger",
        "//common/primitive",
        "//common/bytes",
        "//common/iterator",
        "//resource",
        "//util/project",

        "@crates//:bincode",
        "@crates//:chrono",
        "@crates//:itertools",
        "@crates//:speedb",
        "@crates//:tracing",
        "@crates//:serde",
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
