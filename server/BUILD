# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library")
package(default_visibility = ["//visibility:public",])

rust_library(
    name = "server",
    srcs = glob([
        "*.rs",
        "parameters/**/*.rs",
        "service/*.rs",
    ]),
    deps = [
        "//answer",
        "//common/bytes",
        "//common/error",
        "//common/lending_iterator",
        "//common/options",
        "//compiler",
        "//concept",
        "//database",
        "//user",
        "//encoding",
        "//executor",
        "//function",
        "//ir",
        "//query",
        "//resource",
        "//storage",
        "//system",

        "@typeql//rust:typeql",
        "@typedb_protocol//grpc/rust:typedb_protocol",

        "@crates//:chrono",
        "@crates//:chrono-tz",
        "@crates//:itertools",
        "@crates//:prost",
        "@crates//:tokio",
        "@crates//:tokio-stream",
        "@crates//:tonic",
        "@crates//:tonic-types",
        "@crates//:tracing",
        "@crates//:uuid",
        "@crates//:pwhash",
    ]
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
