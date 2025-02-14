# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_binary", "rust_test")
package(default_visibility = ["//visibility:public",])

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    license_type = "mpl-header",
    exclude = glob([
        "Cargo.*",
    ]),
)

rust_test(
    name = "transformation",
    crate_root = "transformation.rs",
    srcs =  ["transformation.rs"],
    deps = [
        "//compiler:compiler",
        "//concept:concept",
        "//encoding:encoding",
        "//ir:ir",
        "//storage:storage",
        
        "//concept/tests:test_utils_concept",
        "//encoding/tests:test_utils_encoding",
        "//util/test:test_utils",
    
        "@typeql//rust:typeql",

        "@crates//:itertools",
    ],
)

