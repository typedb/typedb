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
load("@rules_rust//rust:defs.bzl", "rust_test", "rust_binary")
package(default_visibility = ["//visibility:public",])

# To run this via Bazel, Criterion must be provided the --bench argument:
#   bazel run --compilation_mode=opt //concept/benches:bench_thing_write -- --bench
rust_test(
    name = "bench_thing_write",
    srcs = glob([
        "bench_thing_write.rs",
    ]),
    deps = [
        "//concept",
        "//encoding",
        "//resource",
        "//storage",
        "//storage/durability",
        "//util/test:test_utils",

        "@crates//:criterion",
        "@crates//:rand",
        "@crates//:pprof",
    ],
    use_libtest_harness = False,
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl-header",
)
