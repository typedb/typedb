# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

load("@typedb_dependencies//tool/checkstyle:rules.bzl", "checkstyle_test")
load("@rules_rust//rust:defs.bzl", "rust_test")

rust_test(
    name = "test_query",
    srcs = glob(["lib.rs", "language/*.rs", "functions/*.rs"]),
    deps = [
        "//tests/behaviour/steps:steps",
        "@crates//:tokio",
    ],
    data = [
        "@typedb_behaviour//query/functions:basic.feature",
        "@typedb_behaviour//query/functions:definition.feature",
        "@typedb_behaviour//query/functions:negation.feature",
        "@typedb_behaviour//query/functions:recursion.feature",
        "@typedb_behaviour//query/functions:signature.feature",
        "@typedb_behaviour//query/functions:structure.feature",
        "@typedb_behaviour//query/functions:usage.feature",

        "@typedb_behaviour//query/language:define.feature",
        "@typedb_behaviour//query/language:delete.feature",
        "@typedb_behaviour//query/language:expression.feature",
        "@typedb_behaviour//query/language:fetch.feature",
        "@typedb_behaviour//query/language:insert.feature",
        "@typedb_behaviour//query/language:match.feature",
        "@typedb_behaviour//query/language:modifiers.feature",
        "@typedb_behaviour//query/language:negation.feature",
        "@typedb_behaviour//query/language:pipelines.feature",
        "@typedb_behaviour//query/language:put.feature",
        "@typedb_behaviour//query/language:redefine.feature",
        "@typedb_behaviour//query/language:reduce.feature",
        "@typedb_behaviour//query/language:undefine.feature",
        "@typedb_behaviour//query/language:update.feature",


    ],
    crate_features = ["bazel"],
    env = {"RUST_MIN_STACK" : "40960000"} # for recursion.feature
)

checkstyle_test(
    name = "checkstyle",
    include = glob(["*", "*/*.rs"]),
    exclude = glob([
        "Cargo.*",
    ]),
    license_type = "mpl-header",
)
