# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_test")
package(default_visibility = ["//visibility:public",])

rust_test(
    name = "basic",
    crate_root = "basic.rs",
    srcs =  ["basic.rs"],
    deps = [
        "//durability:durability",
        "//durability/tests/common:durability_test_common",

        "@crates//:bincode",
        "@crates//:tempdir",
    ],
)

rust_test(
    name = "crash",
    crate_root = "crash.rs",
    srcs = ["crash.rs"],
    env = {
        "TEST_WAL_STREAMER": "$(rootpath //durability/tests/crash/streamer:streamer)",
        "TEST_WAL_RECOVERER": "$(rootpath //durability/tests/crash/recoverer:recoverer)",
    },
    data = [
        "//durability/tests/crash/streamer:streamer",
        "//durability/tests/crash/recoverer:recoverer"
    ],
    deps = [
        "//durability/tests/common:durability_test_common",
        "@crates//:tempdir"
    ],
    use_libtest_harness = False,
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
