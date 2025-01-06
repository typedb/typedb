# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")

rust_test(
    name = "bench_iam",
    srcs = ["bench_iam.rs"],
    deps = [
        "//answer",
        "//common/lending_iterator",
        "//common/options",
        "//database",
        "//executor",
        "//function",
#        "//ir",
        "//query",
#        "//resource",
        "//storage",
#        "//system",

        "//concept/tests:test_utils_concept",
        "//encoding/tests:test_utils_encoding",
        "//util/test:test_utils",

        "@typeql//rust:typeql",
    ],
    data = [":functions.tql", ":schema.tql", ":data.tql", ":debug_schema.tql"],
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
