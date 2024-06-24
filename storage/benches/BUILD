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
        "//durability",
        "//util/test:test_utils",

        "@crates//:rand",
        "@crates//:tracing",
        "@crates//:criterion",
        "@crates//:pprof",
    ],
    use_libtest_harness = False,
)

# Usage:
#  bazel test //storage/benches:bench_rocks --test_output=streamed --cache_test_results=no --test_arg=threads=1 --test_arg=batch_size=64 --test_arg=batches=10000 --test_arg=database=rocks [rocks_disable_wal=true|rocks_set_sync=true]
#  cargo test --release  --bench bench_rocks --package storage -- threads=1 batches=10000 batch_size=64 database=typedb
rust_test(
    name = "bench_rocks",
    crate_root = "bench_rocks.rs",
    srcs = glob([
        "bench_rocks.rs",
        "bench_rocks_impl/**/*.rs",
    ]),
    deps = [
        "@crates//:rand",
        "@crates//:rocksdb",
        "@crates//:rand_core",
        "@crates//:xoshiro",
        "@crates//:itertools",

        "//common/bytes",
        "//database:database",
        "//durability",
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
