# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@vaticle_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library")
package(default_visibility = ["//visibility:public"])

rust_library(
    name = "resource",
    crate_root = "resource.rs",
    srcs = glob([
        "*.rs",
    ]),
    compile_data = [
        "typedb-ascii.txt",
    ],
    deps = [
    ],
)

filegroup(
    name = "logo",
    srcs = ["typedb-ascii.txt"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
        "typedb-ascii.txt",
    ]),
    license_type = "mpl-header",
)
