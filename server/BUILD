# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@bazel_skylib//rules:common_settings.bzl", "string_flag")
load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_library", "rust_test")
package(default_visibility = ["//visibility:public",])

string_flag(
    name = "mode",
    build_setting_default = "development",
)

config_setting(
    name = "published_mode",
    flag_values = {
        ":mode": "published",
    },
    visibility = ["//visibility:public"],
)

config_setting(
    name = "development_mode",
    flag_values = {
        ":mode": "development",
    },
    visibility = ["//visibility:public"],
)

rust_library(
    name = "server",
    srcs = glob([
        "*.rs",
        "authentication/**/*.rs",
        "parameters/**/*.rs",
        "service/**/*.rs",
    ]),
    deps = [
        "//answer",
        "//common/bytes",
        "//common/concurrency",
        "//common/error",
        "//common/lending_iterator",
        "//common/logger",
        "//common/options",
        "//compiler",
        "//concept",
        "//database",
        "//diagnostics",
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

        "@crates//:axum",
        "@crates//:axum-extra",
        "@crates//:axum-server",
        "@crates//:clap",
        "@crates//:chrono",
        "@crates//:chrono-tz",
        "@crates//:futures",
        "@crates//:http",
        "@crates//:hyper",
        "@crates//:itertools",
        "@crates//:jsonwebtoken",
        "@crates//:moka",
        "@crates//:prost",
        "@crates//:rand",
        "@crates//:rustls-pemfile",
        "@crates//:serde",
        "@crates//:serde_json",
        "@crates//:serde_with",
        "@crates//:serde_yaml2",
        "@crates//:tokio",
        "@crates//:tokio-rustls",
        "@crates//:tokio-stream",
        "@crates//:tonic",
        "@crates//:tonic-types",
        "@crates//:tower",
        "@crates//:tower-http",
        "@crates//:tracing",
        "@crates//:uuid",
        "@crates//:pwhash",
        "@crates//:yaml-rust2",
        "@crates//:xxhash-rust",
    ],
    crate_features = select({
        ":development_mode": [],
        ":published_mode": ["published"],
    }),
    tags = [
        "declared-features=published",
    ]
)

rust_test(
    name = "test_crate_server",
    crate = ":server",
    deps = [],
    data = [":config.yml"],
    crate_features = ["bazel"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*", "*/*/*", "*/*/*/*", "*/*/*/*/*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
