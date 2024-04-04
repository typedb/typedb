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

package(default_visibility = ["//tests/behaviour:__subpackages__"])

load("@rules_rust//rust:defs.bzl", "rust_library")

rust_library(
    name = "steps",
    srcs = glob(["**/*.rs"]),
    crate_root = "lib.rs",
    deps = [
        "//database:database",
        "//server:server",
        "//storage/durability:durability",
        "//util/test:test_utils",

        "@crates//:cucumber",
        "@crates//:macro_rules_attribute",
    ],
)

