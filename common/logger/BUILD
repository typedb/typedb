# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rustfmt_test", "rust_test")

package(default_visibility = ["//visibility:public"])

rust_library(
    name = "logger",
    crate_root = "logger.rs",
    srcs = glob([
        "*.rs",
    ]),
    deps = [
        "@crates//:tracing",
        "@crates//:tracing-appender",
        "@crates//:tracing-subscriber",
    ]
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":logger",
    ],
    size = "small",
)

rust_test(
    name = "test_crate_logger",
    crate = ":logger",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    exclude = glob([
        "log_panic.rs",
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)

checkstyle_test(
    name = "checkstyle-copies",
    include = ["log_panic.rs"],
    exclude = [],
    license_type = "apache-header",
)
