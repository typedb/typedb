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
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_test")
package(default_visibility = ["//visibility:public",])

rust_test(
    name = "basic",
    crate_root = "basic.rs",
    srcs = glob(["common/*.rs"]) + ["basic.rs"],
    deps = [
        "//storage/durability:durability",
        "@crates//:bincode",
        "@crates//:tempdir",
    ],
)

rust_binary(
    name = "streamer",
    crate_root = "streamer.rs",
    srcs = glob(["common/*.rs"]) + ["streamer.rs"],
    deps = [
        "//storage/durability:durability",
        "@crates//:bincode",
        "@crates//:itertools",
    ],
)

rust_binary(
    name = "recoverer",
    crate_root = "recoverer.rs",
    srcs = glob(["common/*.rs"]) + ["recoverer.rs"],
    deps = [
        "//storage/durability:durability",
        "@crates//:bincode",
    ],
)

rust_test(
    name = "crash",
    crate_root = "crash.rs",
    srcs = ["crash.rs"],
    env = {
        "TEST_WAL_STREAMER": "$(rootpath :streamer)",
        "TEST_WAL_RECOVERER": "$(rootpath :recoverer)",
    },
    data = [":streamer", ":recoverer"],
    deps = ["@crates//:tempdir"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "agpl-header",
)
