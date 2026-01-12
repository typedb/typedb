# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rustfmt_test")

rust_test(
    name = "test_debug",
    srcs = ["debug.rs"],
    deps = [
        "//tests/behaviour/steps:steps",

        "@crates//:tokio",
    ],
    data = ["debug.feature"],
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":test_debug",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)

exports_files(["debug.feature"])
