# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test", "rustfmt_test")

rust_test(
    name = "test_http_driver_concept",
    srcs = ["concept.rs"],
    deps = [
        "//tests/behaviour/service/http/http_steps:http_steps",

        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//driver:concept.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_http_driver_connection",
    srcs = ["connection.rs"],
    deps = [
        "//tests/behaviour/service/http/http_steps:http_steps",

        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//driver:connection.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_http_driver_http",
    srcs = ["http.rs"],
    deps = [
        "//tests/behaviour/service/http/http_steps:http_steps",

        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//driver/http:http.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_http_driver_query",
    srcs = ["query.rs"],
    deps = [
        "//tests/behaviour/service/http/http_steps:http_steps",

        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//driver:query.feature"],
    crate_features = ["bazel"],
)

rust_test(
    name = "test_http_driver_user",
    srcs = ["user.rs"],
    deps = [
        "//tests/behaviour/service/http/http_steps:http_steps",

        "@crates//:tokio",
    ],
    data = ["@typedb_behaviour//driver:user.feature"],
    crate_features = ["bazel"],
)

rustfmt_test(
    name = "rustfmt_test",
    targets = [
        ":test_http_driver_concept",
        ":test_http_driver_connection",
        ":test_http_driver_http",
        ":test_http_driver_query",
        ":test_http_driver_user",
    ],
    size = "small",
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
