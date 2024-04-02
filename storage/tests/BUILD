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

rust_test(
    name = "test_storage",
    srcs = glob([
        "test_storage.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/logger",
        "//common/primitive",
        "//storage",
        "//storage/durability",
        "//resource",
        "//util/test:test_utils",

        "@crates//:itertools",
        "@crates//:rand",
        "@crates//:tracing",
    ]
)

rust_test(
    name = "test_snapshot",
    srcs = glob([
        "test_snapshot.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/logger",
        "//common/primitive",
        "//storage",
        "//storage/durability",
        "//resource",
        "//util/test:test_utils",
        "@crates//:rand",
        "@crates//:tracing",
    ]
)

rust_test(
    name = "test_mvcc",
    srcs = glob([
        "test_mvcc.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/logger",
        "//common/primitive",
        "//storage",
        "//storage/durability",
        "//resource",
        "//util/test:test_utils",
        "@crates//:rand",
        "@crates//:tracing",
    ]
)

rust_test(
    name = "test_isolation",
    srcs = glob([
        "test_isolation.rs",
    ]),
    deps = [
        "//common/bytes",
        "//common/logger",
        "//common/primitive",
        "//storage",
        "//storage/durability",
        "//resource",
        "//util/test:test_utils",

        "@crates//:rand",
        "@crates//:tracing",
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "agpl-header",
)
