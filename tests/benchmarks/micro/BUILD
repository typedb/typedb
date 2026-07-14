# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test", "rustfmt_test")
package(default_visibility = ["//visibility:public",])

rust_library(
    name = "lib_common",
    srcs = ["common.rs"],
    crate_root = "common.rs",
    deps = [

    ]
)

# To run this via Bazel, Criterion must be provided the --bench argument:
#   bazel run --compilation_mode=opt //tests/benchmarks/micro:microbench_<whatever> -- --bench

rust_test(
    name = "microbench_writes",
    srcs = ["writes.rs"],
    deps = [
        ":lib_common",

        "@crates//:rand",
        "@crates//:criterion",
        "@crates//:pprof",
        "@crates//:xoshiro",
    ],
    tags = ["manual"],  # in order for bazel test //... to not fail
    use_libtest_harness = False,
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    license_type = "mpl-header",
)
