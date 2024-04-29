# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")
package(default_visibility = ["//visibility:public",])

# To run this via Bazel, Criterion must be provided the --bench argument:
#   bazel run --compilation_mode=opt //storage/benches:bench_mvcc_storage -- --bench
rust_test(
    name = "bench_mvcc_storage",
    srcs = glob([
        "bench_mvcc_storage.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/logger",
        "//common/primitive",
        "//resource",
        "//storage",
        "//storage/durability",
        "//util/test:test_utils",

        "@crates//:rand",
        "@crates//:tracing",
        "@crates//:criterion",
        "@crates//:pprof",
    ],
    use_libtest_harness = False,
)

rust_test(
    name = "bench_rocks",
    srcs = glob([
        "bench_rocks.rs", "bench_rocks_impl/**/*.rs",
    ]),
    deps = [
        "@crates//:rand",
        "@crates//:speedb",

        "//common/bytes",
        "//database:database",
        "//storage/durability:durability",
        "//storage:storage",
        "//util/test:test_utils",
    ],
    use_libtest_harness = False,
)


checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
