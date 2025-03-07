# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rust_library")
package(default_visibility = ["//visibility:public",])

rust_library(
    name = "test_utils_concept",
    srcs = ["test_utils_concept.rs"],
    deps = [
        "//concept",
        "//encoding",
        "//storage",
        "//durability",
    ]
)

test_deps = [
    "//common/bytes",
    "//common/lending_iterator",
    "//common/logger",
    "//common/primitive",
    "//concept",
    "//durability",
    "//encoding",
    "//resource",
    "//storage",

    ":test_utils_concept",
    "//encoding/tests:test_utils_encoding",
    "//util/test:test_utils",

    "@crates//:itertools",
    "@crates//:rand",
    "@crates//:tracing",
]

rust_test(
    name = "test_statistics",
    srcs = glob([
        "test_statistics.rs",
    ]),
    deps = test_deps,
)

rust_test(
    name = "test_thing",
    srcs = glob([
        "test_thing.rs",
    ]),
    deps = test_deps,
)

rust_test(
    name = "test_type",
    srcs = glob([
        "test_type.rs",
    ]),
    deps = test_deps + [
        "@crates//:chrono",
        "@crates//:chrono-tz",
    ],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
