# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rust_binary")
package(default_visibility = ["//visibility:public",])

# bazel test --compilation_mode=opt //durability/benches:throughput --test_arg=--bench --test_output=streamed
rust_test(
    name = "throughput",
    srcs = ["throughput.rs"],
    deps = [
        "//durability:durability",

        "@crates//:bincode",
        "@crates//:criterion",
        "@crates//:itertools",
        "@crates//:tempdir",
    ],
    use_libtest_harness = False,
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)

