# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_binary", "rustfmt_test")
package(default_visibility = ["//visibility:public",])

# To run this via Bazel, Criterion must be provided the --bench argument:
#   bazel run --compilation_mode=opt //tests/benchmarks/micro:microbench_<whatever> -- --bench
rust_binary(
    name = "microbench_writes",
    srcs = glob(["*.rs"]),
    deps = [
        "//tests/benchmarks/micro:lib_benchmark",

        "//common/options",
        "//common/primitive",
        "//compiler",
        "//database",
        "//diagnostics",
        "//executor",
        "//query",
        "//storage",
        "//util/test:test_utils",

        "@crates//:rand",
        "@crates//:criterion",
        "@crates//:itertools",
        "@crates//:pprof",
        "@crates//:xoshiro",
        "@typeql//rust:typeql",
    ],
    tags = ["manual"],  # in order for bazel test //... to not fail
#     use_libtest_harness = False,
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":microbench_writes",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob([
        "*",
    ]),
    license_type = "mpl-header",
)
