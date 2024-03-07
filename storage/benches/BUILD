#
# Copyright (C) 2022 Vaticle
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

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
        "//resource",
        "//storage",
        "//util/test:test_utils",

        "@crates//:rand",
        "@crates//:tracing",
        "@crates//:criterion",
    ],
    use_libtest_harness = False,
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl-header",
)
