# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rustfmt_test")

# Run all workloads, or one of: write | read | mixed
#   bazel run --compilation_mode=opt //tests/benchmarks/vector_ops:bench_vector -- read
rust_test(
    name = "bench_vector",
    srcs = ["bench_vector.rs"],
    deps = [
        "//common/options",
        "//database",
        "//diagnostics",
        "//executor",
        "//query",
        "//storage",

        "//util/test:test_utils",

        "@typeql//rust:typeql",

        "@crates//:rand",
    ],
    use_libtest_harness = False,
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":bench_vector",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
