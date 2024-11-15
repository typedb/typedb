# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_test")
package(default_visibility = ["//visibility:public",])

rust_test(
    name = "pattern",
    crate_root = "pattern.rs",
    srcs =  ["pattern.rs"],
    deps = [
        "//ir:ir",
        "@typeql//rust:typeql",
    ],
)

rust_test(
    name = "pipeline",
    crate_root = "pipeline.rs",
    srcs =  ["pipeline.rs"],
    deps = [
        "//answer",
        "//common/bytes",
        "//encoding",
        "//ir:ir",
        "@typeql//rust:typeql",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
)
