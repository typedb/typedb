# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rustfmt_test")
package(default_visibility = ["//visibility:public",])

# To run this via Bazel, Criterion must be provided the --bench argument:
#   bazel run --compilation_mode=opt //executor/benches:bench_instructions -- --bench
#
# To run a single bench function, append a filter pattern:
#   bazel run --compilation_mode=opt //executor/benches:bench_instructions -- --bench "has_reverse_unbound/multi_type"
#
# To generate a flamegraph instead of timing, pass --profile-time <seconds>.
# Criterion will run each matched bench under pprof's SIGPROF sampler and write
# a flamegraph.svg per bench function.
#   bazel run --compilation_mode=opt //executor/benches:bench_instructions \
#       -- --bench --profile-time 30 "has_reverse_unbound/multi_type"
rust_test(
    name = "bench_instructions",
    srcs = glob([
        "bench_instructions.rs",
    ]),
    deps = [
        "//answer",
        "//compiler",
        "//concept",
        "//encoding",
        "//executor",
        "//ir",
        "//resource",
        "//storage",

        "//concept/tests:test_utils_concept",
        "//encoding/tests:test_utils_encoding",
        "//util/test:test_utils",

        "@crates//:criterion",
        "@crates//:pprof",
    ],
    use_libtest_harness = False,
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":bench_instructions",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "mpl-header",
)
