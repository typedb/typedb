# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rust_binary")
package(default_visibility = ["//visibility:public",])

# To run this via Bazel, Criterion must be provided the --bench argument:
#   bazel run --compilation_mode=opt //executor/benches:bench_insert_queries -- --bench
rust_test(
    name = "bench_insert_queries",
    srcs = glob([
        "bench_insert_queries.rs",
    ]),
    deps = [
        "//answer",
        "//common/lending_iterator",
        "//compiler",
        "//concept",
        "//common/lending_iterator",
        "//durability",
        "//encoding",
        "//executor",
        "//ir",
        "//resource",
        "//storage",

        "//concept/tests:test_utils_concept",
        "//encoding/tests:test_utils_encoding",
        "//util/test:test_utils",

        "@vaticle_typeql//rust:typeql",

        "@crates//:criterion",
        "@crates//:rand",
        "@crates//:pprof",
    ],
    use_libtest_harness = False,
)

# To run this via Bazel, Criterion must be provided the --bench argument:
#   bazel run --compilation_mode=opt //executor/benches:bench_insert_queries -- --bench
rust_test(
    name = "bench_insert_queries_multithreaded",
    srcs = glob([
        "bench_insert_queries_multithreaded.rs",
        "common.rs",
    ]),
    deps = [
        "//answer",
        "//common/lending_iterator",
        "//compiler",
        "//concept",
        "//durability",
        "//encoding",
        "//executor",
        "//ir",
        "//resource",
        "//storage",
        "//util/test:test_utils",
        "//concept/tests:test_utils_concept",
        "//encoding/tests:test_utils_encoding",


        "@vaticle_typeql//rust:typeql",

        "@crates//:criterion",
        "@crates//:rand",
        "@crates//:pprof",
    ],
    use_libtest_harness = False,
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "mpl-header",
)
