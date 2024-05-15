# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rust_binary")
package(default_visibility = ["//visibility:public",])

# To run this via Bazel, Criterion must be provided the --bench argument:
#   bazel run --compilation_mode=opt //encoding/benches:benchmark -- --bench
rust_test(
    name = "benchmark",
    srcs = glob([
        "benchmark.rs",
    ]),
    deps = [
        "//encoding",
        "//resource",
        "//storage",
        "//durability",
        "//util/test:test_utils",

        "@crates//:criterion",
    ],
    use_libtest_harness = False,
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "mpl-header",
)
