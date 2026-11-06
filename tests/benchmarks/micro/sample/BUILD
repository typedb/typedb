# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_library", "rustfmt_test")
package(default_visibility = ["//visibility:public",])

rust_binary(
    name = "microbench_samples",
    srcs = glob(["*.rs"]),
    crate_root = "main.rs",
    deps = [
        "//tests/benchmarks/micro/common:lib_benchmark",

        "//common/options",
        "//database",
        "//query",
        "//storage",

        "@crates//:clap",
    ],
    tags = ["exclusive"],
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [":microbench_samples",],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
