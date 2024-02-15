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
    name = "test-storage",
    srcs = glob([
        "test_storage.rs",
    ]),
    deps = [
        "//storage",
        "//common/logger",
        "//util/test:test-utils",

        "@crates//:rand",
        "@crates//:tracing",
    ]
)

rust_test(
    name = "test-snapshot",
    srcs = glob([
        "test_snapshot.rs",
    ]),
    deps = [
        "//storage",
        "//common/logger",
        "//util/test:test-utils",

        "@crates//:rand",
        "@crates//:tracing",
    ]
)

rust_test(
    name = "test-isolation",
    srcs = glob([
        "test_isolation.rs",
    ]),
    deps = [
        "//storage",
        "//common/logger",
        "//util/test:test-utils",

        "@crates//:rand",
        "@crates//:tracing",
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    license_type = "agpl-header",
)
