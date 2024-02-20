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

load("@rules_rust//rust:defs.bzl", "rust_library")
load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
package(default_visibility = [
    ":__subpackages__",
    "//concept:__pkg__",
    "//database:__subpackages__"
])

rust_library(
    name = "encoding",
    crate_root = "encoding.rs",
    srcs = glob([
        "*.rs",
        "*/*.rs",
        "*/*/*.rs",
    ]),
    deps = [
        "//storage",
        "//storage/durability",
        "//common/logger",
        "//common/bytes",

        "@crates//:tracing",
        "@crates//:struct_deser",
    ],
    proc_macro_deps = [
        "@crates//:struct_deser-derive"
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*"]),
    license_type = "agpl-header",
)
